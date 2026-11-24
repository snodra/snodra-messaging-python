from google.protobuf import descriptor_pb2
from snodra._hubpb import hub_pb2, hub_pb2_grpc


def test_bindings_are_hub_proto() -> None:
    p = descriptor_pb2.FileDescriptorProto()
    hub_pb2.DESCRIPTOR.CopyToProto(p)
    assert p.package == "snodra.hub.v1"
    methods = {m.name: m for m in p.service[0].method}
    assert methods["Connect"].client_streaming
    assert methods["Connect"].server_streaming
    assert "Topology" in methods
    register = next(m for m in p.message_type if m.name == "Register")
    assert {f.name: f.number for f in register.field}["subscription_query_types"] == 14
    assert hasattr(hub_pb2_grpc, "HubStub")
    assert hasattr(hub_pb2_grpc, "HubServicer")
    assert hasattr(hub_pb2_grpc, "add_HubServicer_to_server")
