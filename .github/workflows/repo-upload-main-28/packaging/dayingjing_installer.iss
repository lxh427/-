#define MyAppName "达盈镜v1.0"
#define MyAppVersion "1.0.0"
#define MyAppPublisher "DaYingJing"
#define MyAppExeName "DaYingJingLauncher.exe"
#define MyAppServerExeName "DaYingJingServer.exe"

[Setup]
AppId={{7C593DCA-2F7D-4D25-B6C7-C32A9E98526B}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={autopf}\DaYingJing
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes
PrivilegesRequired=lowest
OutputDir=..\dist
OutputBaseFilename=达盈镜v1.0-windows
Compression=lzma2
SolidCompression=yes
WizardStyle=modern

[Languages]
Name: "chinesesimp"; MessagesFile: "compiler:Default.isl"

[Files]
Source: "..\dist\{#MyAppExeName}"; DestDir: "{app}"; Flags: ignoreversion
Source: "..\dist\{#MyAppServerExeName}"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{autoprograms}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "启动达盈镜v1.0"; Flags: nowait postinstall skipifsilent
