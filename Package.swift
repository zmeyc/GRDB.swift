import PackageDescription

let package = Package(
  name: "GRDB",
  dependencies: [
      .Package(url: "https://github.com/groue/CSQLite.git", majorVersion: 3, minor: 8)
  ]
) 
