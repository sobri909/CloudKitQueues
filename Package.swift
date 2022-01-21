// swift-tools-version:5.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "CloudKitQueues",
    platforms: [.iOS(.v14), .macOS(.v11)],
    products: [
        .library(name: "CloudKitQueues", targets: ["CloudKitQueues"])
    ],
    targets: [
        .target(name: "CloudKitQueues", path: "Source")
    ]
)
