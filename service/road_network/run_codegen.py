from grpc_tools import protoc

protoc.main(
    (
        "",
        "-I../road_network/proto",
        "--python_out=.",
        "--grpc_python_out=.",
        "--pyi_out=.",
        "lrs.proto",
    )
)
