# Build context: repository root (docker build -f src/TelemetryCore.LoadGenerator/Dockerfile .)
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY global.json Directory.Build.props Directory.Packages.props ./
COPY src/TelemetryCore.Contracts/TelemetryCore.Contracts.csproj src/TelemetryCore.Contracts/
COPY src/TelemetryCore.Domain/TelemetryCore.Domain.csproj src/TelemetryCore.Domain/
COPY src/TelemetryCore.LoadGenerator/TelemetryCore.LoadGenerator.csproj src/TelemetryCore.LoadGenerator/
RUN dotnet restore src/TelemetryCore.LoadGenerator/TelemetryCore.LoadGenerator.csproj
COPY src/TelemetryCore.Contracts src/TelemetryCore.Contracts
COPY src/TelemetryCore.Domain src/TelemetryCore.Domain
COPY src/TelemetryCore.LoadGenerator src/TelemetryCore.LoadGenerator
RUN dotnet publish src/TelemetryCore.LoadGenerator/TelemetryCore.LoadGenerator.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0
WORKDIR /app
COPY --from=build /app .
USER app
ENTRYPOINT ["dotnet", "TelemetryCore.LoadGenerator.dll"]
