defmodule XMAVLink.Application do
  @moduledoc false

  use Application

  def start(_, _) do
    router_name = Application.get_env(:xmavlink, :router_name, XMAVLink.Router) || XMAVLink.Router

    # The UART pool serves every router's serial connections, the
    # default router's and those of an embedding application that starts
    # routers of its own (a proxy running several), so it is started
    # whether or not the default router is.
    children =
      [uart_pool_spec()] ++
        if Application.get_env(:xmavlink, :start_default_router, true) do
          [XMAVLink.Supervisor] ++ utility_child_specs(router_name)
        else
          []
        end

    Supervisor.start_link(children, strategy: :one_for_one)
  end

  defp uart_pool_spec do
    :poolboy.child_spec(
      :worker,
      name: {:local, XMAVLink.UARTPool},
      worker_module: Circuits.UART,
      size: 0,
      # How many serial ports might you need?
      max_overflow: 10
    )
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
