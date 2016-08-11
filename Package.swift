import PackageDescription

let package = Package(
    name: "GRDB.swift",
    dependencies: [
        .Package(url: "https://github.com/zmeyc/CSQLite.git", majorVersion: 0)
    ],
    exclude: ["GRDB.xcworkspace", "Playgrounds", "SQLCipher", "SQLiteCustom", "Support", "DemoApps", "Tests"]
)
