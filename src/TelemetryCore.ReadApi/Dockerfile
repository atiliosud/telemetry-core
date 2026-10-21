FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY global.json Directory.Build.props Directory.Packages.props ./
COPY src/TelemetryCore.Contracts/TelemetryCore.Contracts.csproj src/TelemetryCore.Contracts/
COPY src/TelemetryCore.ReadApi/TelemetryCore.ReadApi.csproj src/TelemetryCore.ReadApi/
RUN dotnet restore src/TelemetryCore.ReadApi/TelemetryCore.ReadApi.csproj
COPY src/TelemetryCore.Contracts/ src/TelemetryCore.Contracts/
COPY src/TelemetryCore.ReadApi/ src/TelemetryCore.ReadApi/
RUN dotnet publish src/TelemetryCore.ReadApi/TelemetryCore.ReadApi.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
USER app
# No curl/wget in the base image: the API binary probes its own liveness endpoint.
HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=6 CMD ["dotnet", "/app/TelemetryCore.ReadApi.dll", "--healthcheck"]
ENTRYPOINT ["dotnet", "TelemetryCore.ReadApi.dll"]
