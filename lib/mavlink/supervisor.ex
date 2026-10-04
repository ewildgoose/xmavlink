defmodule XMAVLink.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: :"XMAVLink.Supervisor")
  end

  @impl true
  def init(_) do
    router_name = Application.get_env(:xmavlink, :router_name, XMAVLink.Router) || XMAVLink.Router
    router_config = XMAVLink.Router.Config.from_application_env()

    # The UART pool the serial connections use is the application's
    # (XMAVLink.Application), so that it is there for routers started
    # outside this supervisor too.
    children = [{XMAVLink.Router, router_config}] ++ heartbeat_child_specs(router_name)

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp heartbeat_child_specs(router_name) do
    specs = heartbeat_specs()
    multi? = length(specs) > 1

    specs
    |> Enum.with_index()
    |> Enum.map(fn {spec, index} ->
      child_spec =
        spec
        |> Keyword.put_new(:router, router_name)
        |> maybe_unnamed_heartbeat(multi?)

      %{
        id: Keyword.get(spec, :id, {XMAVLink.Heartbeat, index}),
        start: {XMAVLink.Heartbeat, :start_link, [child_spec]}
      }
    end)
  end

  defp maybe_unnamed_heartbeat(spec, true), do: Keyword.put_new(spec, :name, nil)
  defp maybe_unnamed_heartbeat(spec, false), do: spec

  # Heartbeats are started only when configured, so existing apps that
  # build their own heartbeats keep working unchanged. `:heartbeat` keeps
  # the original single-emitter contract; `:heartbeats` allows multiple
  # local source identities to share one router.
  defp heartbeat_specs do
    legacy =
      case Application.get_env(:xmavlink, :heartbeat) do
        nil -> []
        spec -> [spec]
      end

    legacy ++ (Application.get_env(:xmavlink, :heartbeats) || [])
  end
end
