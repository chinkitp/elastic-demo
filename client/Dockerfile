FROM microsoft/dotnet:2.0-sdk-jessie

ADD ./src /code 
WORKDIR /code/ingestor
RUN dotnet restore 
RUN dotnet build
ENTRYPOINT [ "dotnet", "run"] 