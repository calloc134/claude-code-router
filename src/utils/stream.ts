import { Response } from "express";
import { log } from "./log";

export async function streamOpenAIResponse(
  res: Response,
  stream: AsyncIterable<any>,
  model: string,
  body: any
) {
  // 一意のメッセージIDを生成
  const messageId = "msg_" + Date.now();

  // SSE データ送信用ヘルパー
  const write = (data: string) => {
    log("response: ", data);
    res.write(data);
  };

  // 1) message_start イベント
  write(
    `event: message_start
data: ${JSON.stringify({
      type: "message_start",
      message: {
        id: messageId,
        type: "message",
        role: "assistant",
        content: [],
        model,
        stop_reason: null,
        stop_sequence: null,
        usage: { input_tokens: 0, output_tokens: 0 },
      },
    })}

`
  );

  // テキストブロック制御用フラグ
  let hasStartedTextBlock = false;
  const textBlockIndex = 0;

  try {
    // 2) 各種イベントハンドリング
    for await (const event of stream) {
      // サーバー側ログ
      log("event received", event);

      switch (event.type) {
        // --- レスポンス生成ライフサイクル ---
        case "response.created":
          write(
            `event: response.created
data: ${JSON.stringify(event.response)}

`
          );
          break;
        case "response.in_progress":
          write(
            `event: response.in_progress
data: ${JSON.stringify(event.response)}

`
          );
          break;

        // --- reasoning やメッセージブロックの追加/完了 ---
        case "response.output_item.added":
          write(
            `event: response.output_item.added
data: ${JSON.stringify(event.item)}

`
          );
          break;
        case "response.output_item.done":
          write(
            `event: response.output_item.done
data: ${JSON.stringify(event.item)}

`
          );
          break;

        // --- テキストパートの開始・デルタ・終了 ---
        case "response.content_part.added":
          write(
            `event: content_block_start
data: ${JSON.stringify({
              type: "content_block_start",
              index: textBlockIndex,
              content_block: {
                type: "text",
                id: "content-block-" + messageId,
                text: "",
              },
            })}

`
          );
          hasStartedTextBlock = true;
          break;
        case "response.output_text.delta":
          if (!hasStartedTextBlock) {
            // safety: 開始が漏れていたら自動発行
            write(
              `event: content_block_start
data: ${JSON.stringify({
                type: "content_block_start",
                index: textBlockIndex,
                content_block: {
                  type: "text",
                  id: "content-block-" + messageId,
                  text: "",
                },
              })}

`
            );
            hasStartedTextBlock = true;
          }
          write(
            `event: content_block_delta
data: ${JSON.stringify({
              index: textBlockIndex,
              delta: { type: "text_delta", text: event.delta },
            })}

`
          );
          break;
        case "response.output_text.done":
          write(
            `event: content_block_stop
data: ${JSON.stringify({
              type: "content_block_stop",
              index: textBlockIndex,
            })}

`
          );
          break;

        // --- ツール呼び出しブロック ---
        case "response.tool.call":
          write(
            `event: content_block_start
data: ${JSON.stringify({
              type: "content_block_start",
              index: 1,
              content_block: {
                type: "tool_use",
                id: event.tool_call.call_id,
                name: event.tool_call.name,
                input: event.tool_call.arguments,
              },
            })}

`
          );
          break;
        case "response.tool.output_delta":
          write(
            `event: content_block_delta
data: ${JSON.stringify({
              index: 1,
              delta: { type: "output_json_delta", partial_json: event.delta },
            })}

`
          );
          break;
        case "response.tool.call_done":
          write(
            `event: content_block_stop
data: ${JSON.stringify({
              type: "content_block_stop",
              index: 1,
            })}

`
          );
          break;

        // --- エラー／完了通知 ---
        case "response.error":
          write(
            `event: error
data: ${JSON.stringify(event.error)}

`
          );
          res.end();
          return;

        case "response.completed":
          write(
            `event: message_delta
data: ${JSON.stringify({
              type: "message_delta",
              delta: { stop_reason: "end_turn", stop_sequence: null },
            })}

`
          );
          write(
            `event: message_stop
data: ${JSON.stringify({ type: "message_stop" })}

`
          );
          res.end();
          return;
      }
    }
  } catch (err: any) {
    write(
      `event: error
data: ${JSON.stringify({ message: err.message })}

`
    );
    res.end();
  }
}
