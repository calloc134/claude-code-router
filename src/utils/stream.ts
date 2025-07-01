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
  const write = (data: string) => {
    log("response: ", data);
    res.write(data);
  };

  // const messageId = `msg_${Date.now()}`;
  // const contentBlockId = `content-block-${Date.now()}`;
  // let hasTextBlockStarted = false;

  const messageId = `msg_${Date.now()}`;
  const contentBlockId = `content-block-${Date.now()}`;
  let hasTextBlockStarted = false;
  // --- ツール呼び出し用ステートを追加 ---
  let isToolUse = false;
  let toolCallJson = "";
  let currentToolCallId: string | null = null;
  let contentBlockIndex = 0;

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
          const contentDelta = {
            type: "content_block_delta",
            index: 0,
            delta: { type: "text_delta", text: event.delta },
          };
          write(
            `event: content_block_delta\ndata: ${JSON.stringify(
              contentDelta
            )}\n\n`
          );
          break;

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
            toolCallJson += event.tool_call.arguments;
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
          break;

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
        case "response.output_text.done":
          break;
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
    // res.end();

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
    res.end();
  }
}
