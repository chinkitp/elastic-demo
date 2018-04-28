FROM microsoft/dotnet:2.0-sdk-jessie

ADD ./src /code 
WORKDIR /code/ingestor
RUN dotnet restore 
ENTRYPOINT [ "dotnet", "run"] 