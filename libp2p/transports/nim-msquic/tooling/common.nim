## MsQuic 工具链/CI 相关的公共类型，对应 `scripts/`, `cmake/`, `src/test/`, `src/tools/`。

type
  ScriptLanguage* = enum
    slPowerShell
    slShell
    slBash
    slCmd
    slRust
    slCmake
    slNim

  BuildToolchain* = enum
    btCmake
    btNinja
    btMsBuild
    btCargo
    btClang
    btMsvc

  ArtifactKind* = enum
    akStaticLib
    akDynamicLib
    akHeaders
    akTools
    akTests
    akPackages
    akDocs

  PipelineStage* = enum
    psConfigure
    psBuild
    psTest
    psPackage
    psPublish
    psDoc
    psInterop
    psPerf

  TestSuiteKind* = enum
    tsUnit
    tsIntegration
    tsInterop
    tsPerf
    tsFuzz

  ToolCategory* = enum
    tcCliSample
    tcInterop
    tcPerf
    tcDiagnostics
    tcPackaging
    tcAutomation

  ScriptDescriptor* = object
    path*: string
    language*: ScriptLanguage
    summary*: string
    stage*: PipelineStage

  BuildProfile* = object
    name*: string
    description*: string
    cmakePreset*: string
    toolchains*: set[BuildToolchain]
    primaryScripts*: seq[string]
    artifacts*: set[ArtifactKind]

  TestSuiteDescriptor* = object
    name*: string
    kind*: TestSuiteKind
    sources*: seq[string]
    driverScripts*: seq[string]
    notes*: string

  ToolingComponent* = object
    name*: string
    category*: ToolCategory
    sources*: seq[string]
    driver*: string
    notes*: string

  CiJob* = object
    name*: string
    stage*: PipelineStage
    scripts*: seq[string]
    produces*: set[ArtifactKind]
    dependsOn*: seq[string]
    notes*: string
