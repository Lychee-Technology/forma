export class FormaError extends Error {
  public readonly status: number;
  public readonly body: unknown;

  constructor(message: string, status: number, body: unknown) {
    super(message);
    this.name = "FormaError";
    this.status = status;
    this.body = body;
  }
}
