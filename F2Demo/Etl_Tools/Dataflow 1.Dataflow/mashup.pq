[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"Displayed text (2)" = let

  BaseUrl  = "http://192.168.21.49:8088",

  Endpoint = "api/report-period/Year/filter-without-permission/0/1000",

  Token    = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiOWYyOWQ3ZTQtM2FiZi00M2NmLTk0MGYtYjUzMWZmYjc4ODQ0IiwidG9rZW5faW5zdGFuY2VfaWQiOiJoQ2JwNUdzYnh1MXBxZm5IZHlod2poYVdCMmpSNXZmVHp5aFNvcTIiLCJqdGkiOiI2ZmIzM2Y3ZS01N2VjLTRiYzctOTIwYS0zMTRmZTNiMWRkYzYiLCJwcm4iOiJjNzFmNzliZC0zMzVlLTRiNWItOGI3ZS0yOGE4NmMxNDdlNTQiLCJuYmYiOjE3NTU1MDU1MTgsImV4cCI6MTc1NTUwOTExOCwiaWF0IjoxNzU1NTA1NTE4LCJpc3MiOiJodHRwczovL2FwaS1iY3F0LnZpbmNvbS5jb20udm4iLCJhdWQiOiJodHRwczovL2FwaS1iY3F0LnZpbmNvbS5jb20udm4ifQ.1AL8n5MVP6qO1yWQ3w3MzRL8FGfo-QzYnlwTCFMk8u4",

  Source = Json.Document(
    Web.Contents(
      BaseUrl,
      [
        RelativePath = Endpoint,
        Headers = [
          Accept        = "application/json",
          Authorization = "Bearer " & Token
        ],
        Timeout = #duration(0,0,5,0)
      ]
    )
  ),
  Navigation = Source[data],
  #"Navigation 1" = Navigation[items],
  #"Navigation 2" = #"Navigation 1"{0},
  #"Navigation 3" = #"Navigation 2"[financialPlanStatus]
in
  #"Navigation 3";
