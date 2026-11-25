defmodule Mailstore.Imap.Framing do
  @moduledoc """
  Cuts complete commands out of the TCP byte stream. A command ends at CRLF unless
  the line ends with a literal marker `{n}` / `{n+}`; then n raw bytes follow and the
  command continues after them. For a synchronizing literal `{n}` the server must send
  a continuation (`+ ...`) before the client sends the bytes: `next/2` reports that.
  """
  @max_line 64 * 1024
  @max_literal 64 * 1024 * 1024

  # pos: where the scan of the current (incomplete) command stopped;
  # continued_at: literal start we already sent "+" for.
  defstruct pos: 0, continued_at: nil

  def new, do: %__MODULE__{}

  @spec next(binary, %__MODULE__{}) ::
          {:command, binary, rest :: binary, %__MODULE__{}}
          | {:continue, %__MODULE__{}}
          | {:more, %__MODULE__{}}
          | {:error, binary}
  def next(buffer, %__MODULE__{pos: pos} = f) do
    case :binary.match(buffer, "\r\n", scope: {pos, byte_size(buffer) - pos}) do
      :nomatch ->
        if byte_size(buffer) - pos > @max_line, do: {:error, "Line too long"}, else: {:more, f}

      {eol, 2} ->
        line = binary_part(buffer, pos, eol - pos)

        case Regex.run(~r/\{(\d+)(\+?)\}$/, line) do
          [_, digits, plus] ->
            n = String.to_integer(digits)
            start = eol + 2

            cond do
              n > @max_literal ->
                {:error, "Literal too large"}

              byte_size(buffer) >= start + n ->
                next(buffer, %{f | pos: start + n})

              plus == "" and f.continued_at != start ->
                {:continue, %{f | continued_at: start}}

              true ->
                {:more, f}
            end

          nil ->
            size = eol + 2

            {:command, binary_part(buffer, 0, size),
             binary_part(buffer, size, byte_size(buffer) - size), new()}
        end
    end
  end
end
