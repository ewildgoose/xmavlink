defmodule XMAVLink.Application do
  @moduledoc false

  use Application

  def start(_, _) do
    router_name = Application.get_env(:xmavlink, :router_name, XMAVLink.Router) || XMAVLink.Router

    children =
      if Application.get_env(:xmavlink, :start_default_router, true) do
        [XMAVLink.Supervisor] ++ utility_child_specs(router_name)
      else
        # Embedding applications (e.g. proxies running several routers) start
        # their own XMAVLink.Router instances. Note XMAVLink.Supervisor also
        # owns the UART pool, so serial connections require the default
        # supervisor (or an equivalent poolboy pool) to be running.
        []
      end

    Supervisor.start_link(children, strategy: :one_for_one)
  end

  defp utility_child_specs(router_name) do
    case Application.get_env(:xmavlink, :utilities, false) do
      false ->
        []

      nil ->
        []

      true ->
        [{XMAVLink.Util.Supervisor, router: router_name}]

      opts when is_list(opts) ->
        if Keyword.keyword?(opts) do
          opts =
            if Keyword.has_key?(opts, :context) do
              opts
            else
              Keyword.put_new(opts, :router, router_name)
            end

          [{XMAVLink.Util.Supervisor, opts}]
        else
          invalid_utilities_config!(opts)
        end

      invalid ->
        invalid_utilities_config!(invalid)
    end
  end

  defp invalid_utilities_config!(value) do
    raise ArgumentError,
          ":utilities must be true, false, nil, or a keyword list, got: #{inspect(value)}"
  end
end
