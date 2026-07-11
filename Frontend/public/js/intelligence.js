export async function runAgentLoop({ intelligence, provider, model, chatend, tools, executor, onUpdate = () => {} }) {
  for (let round = 0; round < 100; round++) {
    const response = await intelligence.generate({ provider, model, messages: chatend.messages, tools });
    if (response.status === "complete") {
      const content = response.message?.content;
      if (typeof content !== "string") throw new Error("Kennedy returned no final text.");
      chatend.append(response.message); onUpdate(); return content;
    }
    const calls = response.message?.tool_calls || [];
    if (response.status !== "tool_calls" || calls.length === 0) throw new Error("The intelligence service returned an invalid generation state.");
    chatend.append(response.message); onUpdate();
    const resetIsMixed = calls.length > 1 && calls.some(call => call.name === "ResetContext");
    for (const call of calls) {
      const execution = resetIsMixed && call.name === "ResetContext"
        ? executor.failure(call, "mixed_reset_call", "ResetContext must be requested by itself so the chatend can be rebuilt safely.")
        : await executor.execute(call);
      if (execution.reset) chatend.rebuildAfterReset(response.message, execution.message);
      else chatend.append(execution.message);
      onUpdate();
    }
  }
  throw new Error("Kennedy exceeded the 100-round tool-loop safety limit.");
}

