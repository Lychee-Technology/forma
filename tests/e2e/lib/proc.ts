/**
 * Subprocess helper for the Go CDC tool wrappers.
 *
 * Fixes three hazards the per-script inline spawns shared:
 *  - no timeout (a hung tool hung the script forever),
 *  - stdout drained fully before stderr (a large stderr fills the pipe buffer
 *    and deadlocks the child), and
 *  - secrets echoed in the logged command line.
 */

/** Flags whose following argument is a secret and must never be logged. */
const SECRET_FLAGS = new Set(['--pg-password', '--s3-secret-key', '--s3-access-key']);

/**
 * Render an argv for logging with the value after any secret flag replaced by
 * '***'. Handles both "--pg-password value" and "--pg-password=value" forms.
 */
export function redactArgs(args: string[]): string {
  const out: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    const eq = arg.indexOf('=');
    if (eq !== -1 && SECRET_FLAGS.has(arg.slice(0, eq))) {
      out.push(`${arg.slice(0, eq)}=***`);
      continue;
    }
    out.push(arg);
    if (SECRET_FLAGS.has(arg) && i + 1 < args.length) {
      out.push('***');
      i++;
    }
  }
  return out.join(' ');
}

export interface ToolResult {
  exitCode: number;
  stdout: string;
  stderr: string;
  timedOut: boolean;
  durationMs: number;
}

/**
 * Replace every occurrence of each secret value with '***'. Command-line
 * redaction only covers the logged argv; a tool that echoes a credential to
 * stdout/stderr would still surface it in reports/console, so captured output
 * is scrubbed too. Empty/short values are ignored to avoid masking noise.
 */
export function redactSecrets(text: string, secrets: string[]): string {
  let out = text;
  for (const secret of secrets) {
    if (secret && secret.length >= 4) {
      out = out.split(secret).join('***');
    }
  }
  return out;
}

async function drain(stream: ReadableStream<Uint8Array>): Promise<string> {
  const chunks: Uint8Array[] = [];
  const reader = stream.getReader();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  return new TextDecoder().decode(Buffer.concat(chunks));
}

/**
 * Spawn the Go tool, draining stdout and stderr concurrently and enforcing a
 * timeout. On timeout the process is killed and timedOut is set; the caller
 * decides how to report it.
 */
export async function runTool(
  toolPath: string,
  args: string[],
  opts: { env?: Record<string, string | undefined>; timeoutMs: number; redactValues?: string[] },
): Promise<ToolResult> {
  const start = Date.now();
  const proc = Bun.spawn([toolPath, ...args], {
    env: opts.env,
    stdout: 'pipe',
    stderr: 'pipe',
  });

  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    proc.kill();
  }, opts.timeoutMs);

  try {
    const [stdoutRaw, stderrRaw, exitCode] = await Promise.all([
      drain(proc.stdout as ReadableStream<Uint8Array>),
      drain(proc.stderr as ReadableStream<Uint8Array>),
      proc.exited,
    ]);
    const secrets = opts.redactValues ?? [];
    return {
      exitCode,
      stdout: redactSecrets(stdoutRaw, secrets),
      stderr: redactSecrets(stderrRaw, secrets),
      timedOut,
      durationMs: Date.now() - start,
    };
  } finally {
    clearTimeout(timer);
  }
}
