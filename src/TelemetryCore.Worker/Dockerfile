# Build context: repository root (docker build -f src/TelemetryCore.Worker/Dockerfile .)
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY global.json Directory.Build.props Directory.Packages.props ./
COPY src/TelemetryCore.Contracts/TelemetryCore.Contracts.csproj src/TelemetryCore.Contracts/
COPY src/TelemetryCore.Domain/TelemetryCore.Domain.csproj src/TelemetryCore.Domain/
COPY src/TelemetryCore.Worker/TelemetryCore.Worker.csproj src/TelemetryCore.Worker/
RUN dotnet restore src/TelemetryCore.Worker/TelemetryCore.Worker.csproj
COPY src/TelemetryCore.Contracts src/TelemetryCore.Contracts
COPY src/TelemetryCore.Domain src/TelemetryCore.Domain
COPY src/TelemetryCore.Worker src/TelemetryCore.Worker
RUN dotnet publish src/TelemetryCore.Worker/TelemetryCore.Worker.csproj -c Release -o /app --no-restore

# D12: ASP.NET Core runtime image, because the worker serves /metrics and /health/*.
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
USER app
# No curl/wget in the base image: the worker binary probes its own liveness endpoint.
HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=6 CMD ["dotnet", "/app/TelemetryCore.Worker.dll", "--healthcheck"]
ENTRYPOINT ["dotnet", "TelemetryCore.Worker.dll"]
