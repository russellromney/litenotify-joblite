const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const sqlite_prefix = b.option([]const u8, "sqlite-prefix", "Path to a SQLite install prefix, for example /opt/homebrew/opt/sqlite");
    const json_prefix = b.option([]const u8, "json-prefix", "Path to a nlohmann-json install prefix, for example /opt/homebrew/opt/nlohmann-json");

    const honker_mod = b.createModule(.{
        .root_source_file = b.path("src/honker.zig"),
        .target = target,
        .optimize = optimize,
    });

    honker_mod.link_libc = true;
    honker_mod.linkSystemLibrary("sqlite3", .{});
    if (sqlite_prefix) |prefix| {
        honker_mod.addIncludePath(.{ .cwd_relative = b.pathJoin(&.{ prefix, "include" }) });
        honker_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ prefix, "lib" }) });
    }

    const lib = b.addLibrary(.{
        .name = "honker_cpp",
        .root_module = honker_mod,
        .linkage = .static,
    });
    b.installArtifact(lib);

    const ext_opt = b.option([]const u8, "honker-ext", "Path to libhonker_ext.{dylib,so}");

    const test_step = b.step("test", "Run the C++ test suite");

    const test_files = .{
        "test/test_basic.cpp",
        "test/test_parity.cpp",
    };

    inline for (test_files) |tf| {
        const mod = b.createModule(.{
            .target = target,
            .optimize = optimize,
        });
        mod.addCSourceFile(.{
            .file = b.path(tf),
            .flags = &.{"-std=c++17", "-Wall", "-Wextra"},
        });
        mod.addIncludePath(b.path("include"));
        if (json_prefix) |prefix| {
            mod.addIncludePath(.{ .cwd_relative = b.pathJoin(&.{ prefix, "include" }) });
        }
        mod.link_libc = true;
        mod.link_libcpp = true;
        mod.linkSystemLibrary("sqlite3", .{});
        if (sqlite_prefix) |prefix| {
            mod.addIncludePath(.{ .cwd_relative = b.pathJoin(&.{ prefix, "include" }) });
            mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ prefix, "lib" }) });
        }
        mod.linkLibrary(lib);

        const exe = b.addExecutable(.{
            .name = std.fs.path.stem(tf),
            .root_module = mod,
        });
        b.installArtifact(exe);

        const run = b.addRunArtifact(exe);
        if (ext_opt) |ext| {
            run.setEnvironmentVariable("HONKER_EXTENSION_PATH", ext);
        }

        test_step.dependOn(&run.step);
    }
}
