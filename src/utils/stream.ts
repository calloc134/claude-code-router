import { Response } from "express";
import { log } from "./log";

// A simplified interface for the events we care about
interface HandledEvent {
  type: string;
  delta?: { text: string };
  response?: { status: string };
  error?: { message: string };
}

export async function streamOpenAIResponse(
  res: Response,
  stream: any, // The stream from openai.responses.create
  model: string,
  body: any
) {
  // const write = (data: string) => {
  //   log("response: ", data);
  //   res.write(data);
  // };

  // ガード付き書き込み & 終了制御
  let ended = false;
  const write = (data: string) => {
    if (ended) return;
    log("response: ", data);
    res.write(data);
  };
  const safeEnd = () => {
    if (ended) return;
    ended = true;
    safeEnd();
  };

  // const messageId = `msg_${Date.now()}`;
  // const contentBlockId = `content-block-${Date.now()}`;
  // let hasTextBlockStarted = false;

  // const messageId = `msg_${Date.now()}`;
  if (!body.stream && typeof stream[Symbol.asyncIterator] !== "function") {
    const completion = stream as any;
    let content: any[] = [];
    if (completion.choices?.[0]?.message?.content) {
      content = [{ text: completion.choices[0].message.content, type: "text" }];
    } else if (completion.choices?.[0]?.message?.tool_calls) {
      content = completion.choices[0].message.tool_calls.map((tc: any) => ({
        type: "tool_use",
        id: tc.id,
        name: tc.function?.name,
        input: tc.function?.arguments ? JSON.parse(tc.function.arguments) : {},
      }));
    }
    // 従来フォーマットで即時返却
    res.json({
      id: `msg_${Date.now()}`,
      type: "message",
      role: "assistant",
      content,
      stop_reason:
        completion.choices[0].finish_reason === "tool_calls"
          ? "tool_use"
          : "end_turn",
      stop_sequence: null,
    });
    safeEnd();
    return;
  }

  const messageId = `msg_${Date.now()}`;
  const contentBlockId = `content-block-${Date.now()}`;
  let hasTextBlockStarted = false;
  // --- ツール呼び出し用ステートを追加 ---
  let isToolUse = false;
  let toolCallJson = "";
  let currentToolCallId: string | null = null;
  let contentBlockIndex = 0;

  let waitingToolResult = false; // ツールが返答を返してくるか
  let allTextDone = false; // output_text が終わったか

  try {
    // Send message_start event immediately
    const messageStart = {
      type: "message_start",
      message: {
        id: messageId,
        type: "message",
        role: "assistant",
        content: [],
        model,
        stop_reason: null,
        stop_sequence: null,
        usage: { input_tokens: 1, output_tokens: 1 }, // Dummy usage
      },
    };
    write(`event: message_start\ndata: ${JSON.stringify(messageStart)}\n\n`);

    for await (const event of stream) {
      log("event received", JSON.stringify(event, null, 2));

      switch (event.type) {
        case "response.output_text.delta":
          if (!hasTextBlockStarted) {
            // If this is the first text delta, send content_block_start
            const contentBlockStart = {
              type: "content_block_start",
              index: 0,
              content_block: { type: "text", id: contentBlockId, text: "" },
            };
            write(
              `event: content_block_start\ndata: ${JSON.stringify(
                contentBlockStart
              )}\n\n`
            );
            hasTextBlockStarted = true;
          }
          // Send the actual text chunk
          // const contentDelta = {
          //   type: "content_block_delta",
          //   index: 0,
          //   delta: { type: "text_delta", text: event.delta },
          // };
          console.log("event", event);
          // const chunkText = event.delta?.text ?? "";
          const chunkText =
            typeof event.delta === "string"
              ? event.delta
              : event.delta?.text ?? "";
          const contentDelta = {
            type: "content_block_delta",
            index: 0,
            delta: { type: "text_delta", text: chunkText },
          };
          write(
            // `event: content_block_delta\ndata: ${JSON.stringify(
            //   contentDelta
            // )}\n\n`
            `event: content_block_delta\ndata: ${JSON.stringify(
              contentDelta
            )}\n\n`
          );
          break;

        /* output_text が完全に終わった合図 */
        case "response.output_text.done": {
          allTextDone = true;
          if (hasTextBlockStarted) {
            write(
              `event: content_block_stop\ndata:${JSON.stringify({
                type: "content_block_stop",
                index: 0,
              })}\n\n`
            );
            hasTextBlockStarted = false;
          }
          break;
        }
        // --- ツール呼び出しイベントのハンドリングを追加 ---
        case "response.tool_call.started":
        case "response.tool_call.in_progress":
          // ツール呼び出し開始を検知
          if (!isToolUse && event.tool_call?.id) {
            isToolUse = true;
            currentToolCallId = event.tool_call.id;
            toolCallJson = "";
            // SSE: content_block_start（ツールブロック）
            contentBlockIndex++;
            write(
              `event: content_block_start\ndata: ${JSON.stringify({
                type: "content_block_start",
                index: contentBlockIndex,
                content_block: {
                  type: "tool_use",
                  id: currentToolCallId,
                  name: event.tool_call.name,
                  input: {},
                },
              })}\n\n`
            );
          }
          // 引き続き JSON 部分を蓄積
          if (isToolUse && event.tool_call?.arguments) {
            // toolCallJson += event.tool_call.arguments;
            // v5 SDK は arguments.delta または arguments.partial_json を吐く
            const argChunk =
              typeof event.tool_call.arguments === "string"
                ? event.tool_call.arguments
                : event.tool_call.arguments?.partial_json ?? "";
            toolCallJson += argChunk;

            // 配信
            write(
              `event: content_block_delta\ndata:${JSON.stringify({
                type: "content_block_delta",
                index: contentBlockIndex,
                delta: {
                  type: "input_json_delta",
                  partial_json: argChunk,
                },
              })}\n\n`
            );
          }
          break;

        case "response.tool_call.completed":
          if (isToolUse) {
            // JSON をパースして SSE で送信
            try {
              JSON.parse(toolCallJson);
            } catch {}
            write(
              `event: content_block_delta\ndata: ${JSON.stringify({
                type: "content_block_delta",
                index: contentBlockIndex,
                delta: { type: "input_json_delta", partial_json: toolCallJson },
              })}\n\n`
            );
            // ツールブロック終了
            write(
              `event: content_block_stop\ndata: ${JSON.stringify({
                type: "content_block_stop",
                index: contentBlockIndex,
              })}\n\n`
            );
            isToolUse = false;
            toolCallJson = "";
            currentToolCallId = null;
          }
          waitingToolResult = true; // ツール側の result を待つ
          break;

        /* ---------- ツール結果 ---------- */
        case "response.tool_result.started": {
          /* ここではブロック開始はせず delta でまとめ送信する */
          break;
        }

        case "response.tool_result.delta": {
          // Claude-Code 互換: tool_result は text として流す
          const toolText =
            typeof event.delta === "string"
              ? event.delta
              : event.delta?.text ?? "";
          write(
            `event: content_block_delta\ndata:${JSON.stringify({
              type: "content_block_delta",
              index: contentBlockIndex + 1, // result 用に 1 つ先を使う
              delta: { type: "text_delta", text: toolText },
            })}\n\n`
          );
          break;
        }

        case "response.tool_result.completed": {
          // 結果用コンテンツブロックを閉じる
          write(
            `event: content_block_stop\ndata:${JSON.stringify({
              type: "content_block_stop",
              index: contentBlockIndex + 1,
            })}\n\n`
          );
          waitingToolResult = false;
          break;
        }

        case "response.completed":
          if (hasTextBlockStarted) {
            // Stop the text block if it was started
            const contentBlockStop = {
              type: "content_block_stop",
              index: 0,
            };
            write(
              `event: content_block_stop\ndata: ${JSON.stringify(
                contentBlockStop
              )}\n\n`
            );
          }

          // Send message_delta with the final stop reason
          const messageDelta = {
            type: "message_delta",
            delta: {
              stop_reason: "end_turn", // Derived from 'completed' status
              stop_sequence: null,
            },
            usage: { output_tokens: event.response?.usage?.output_tokens || 1 },
          };
          write(
            `event: message_delta\ndata: ${JSON.stringify(messageDelta)}\n\n`
          );
          break;

        case "response.error":
          log("Stream error:", event.error);
          const errorJson = JSON.stringify({
            type: "error",
            error: {
              type: "api_error",
              message: event.error?.message || "Unknown error",
            },
          });
          write(`event: error\ndata: ${errorJson}\n\n`);
          break;

        // Other events are logged but ignored for the client-side stream
        case "response.created":
        case "response.in_progress":
        case "response.web_search_call.in_progress":
        case "response.web_search_call.searching":
        case "response.web_search_call.completed":
          // case "response.output_text.done":
          break;
      }

      /* フェイルセーフ: すべて終わったら自前で close */
      if (allTextDone && !isToolUse && !waitingToolResult) {
        write(
          `event: message_delta\ndata:${JSON.stringify({
            type: "message_delta",
            delta: { stop_reason: "end_turn", stop_sequence: null },
            usage: { output_tokens: 1 },
          })}\n\n`
        );
        write(
          `event: message_stop\ndata:${JSON.stringify({
            type: "message_stop",
          })}\n\n`
        );
        safeEnd();
        return;
      }
    }
  } catch (e: any) {
    // log("Error in stream processing:", e);
    // const errorJson = JSON.stringify({
    //   type: "error",
    //   error: { type: "internal_server_error", message: e.message },
    // });
    // write(`event: error\ndata: ${errorJson}\n\n`);
    log("Error in stream processing:", e);
    const errEvent = {
      type: "error",
      error: { type: "stream_error", message: e.message },
    };
    write(`event: error\ndata: ${JSON.stringify(errEvent)}\n\n`);
    // 非ストリーミング呼び出し時は HTTP 500 も返却
    if (!body.stream) {
      res.status(500).json({ error: e.message });
      return;
    }
  } finally {
    // Finally, send the message_stop event
    // const messageStop = { type: "message_stop" };
    // write(`event: message_stop\ndata: ${JSON.stringify(messageStop)}\n\n`);
    // safeEnd();

    // --- 残ったコンテンツブロックをクローズ ---
    if (isToolUse || hasTextBlockStarted) {
      write(
        `event: content_block_stop\ndata: ${JSON.stringify({
          type: "content_block_stop",
          index: contentBlockIndex,
        })}\n\n`
      );
    }
    // 最終的な message_stop
    const messageStop = { type: "message_stop" };
    write(`event: message_stop\ndata: ${JSON.stringify(messageStop)}\n\n`);
    safeEnd();
  }
}
