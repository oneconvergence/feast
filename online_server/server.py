from py_compile import main
import fastapi
import uvicorn

api = fastapi.FastAPI()

if __name__ == "__main__":
    uvicorn.run(api, port=8081, host="127.0.0.1")
