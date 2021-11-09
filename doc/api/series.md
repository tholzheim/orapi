# Event series API 


## Get a Event series and its event records

Get a single Event series and its events for the given event series page title (in most cases it corresponds to the acronym)

If:

- No event series is found for the given pageTitle an empty result is returned

NOTE:
This endpoint can be accessed without authentication.

```plaintext
GET /api/series/<series>?format=json
```

Parameters:

| Attribute | Type    | Required | Description                                                                                                                             |
|:----------|:--------|:---------|:----------------------------------------------------------------------------------------------------------------------------------------|
| `format`   | string  | no      | format of the returned result. Supported options: 'json', 'spreadsheet', 'application/json', 'application/vnd.oasis.opendocument.spreadsheet'|

Example request:

```shell
curl "https://localhost:8558/api/series/VNC?format=json"
```

Example response:

```json
{
  "avatar_url": "https://www.gravatar.com/avatar/e64c7d89f26bd1972efa854d13d7dd61?s=64&d=identicon"
}
```
