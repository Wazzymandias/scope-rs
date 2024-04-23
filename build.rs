use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    import_protobuf_schemas()?;

    Ok(())
}

fn import_protobuf_schemas() -> Result<(), Box<dyn std::error::Error>> {
    let mut proto_files = Vec::new();

    for entry in glob::glob("proto/schemas/*.proto").unwrap() {
        let file_path = entry.unwrap();
        proto_files.push(file_path);
    }

    let proto_include_dirs = ["proto/schemas/"];

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .out_dir("src/") // Output directory for generated Rust files
        .emit_rerun_if_changed(true)
        .compile(&proto_files, &proto_include_dirs)?;

    fs::rename("src/_.rs", "src/pb.rs").unwrap();

    Ok(())
}
