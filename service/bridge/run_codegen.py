from grpc_tools import protoc

protoc.main(
    (
        "",
        "-I../bridge/proto",
        "--python_out=.",
        "--grpc_python_out=.",
        "--pyi_out=.",
        "bridge_master.proto",
    )
)
