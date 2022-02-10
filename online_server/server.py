import fastapi
import uvicorn

from apis import feast_api

api = fastapi.FastAPI()

if __name__ == "__main__":
    api.include_router(feast_api.router)
    uvicorn.run(api, port=8081, host="127.0.0.1")
