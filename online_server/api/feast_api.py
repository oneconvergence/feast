import fastapi

router = fastapi.APIRouter()


@router.post("/api/v1/materialize", name="materialize", status_code=201)
def materialize():
    pass
