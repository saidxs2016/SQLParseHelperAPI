from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlglot import parse_one, transpile
from sqlglot.errors import ParseError
from sqlglot.expressions import Order  # Order sınıfını içe aktarın

app = FastAPI()

# Desteklenen SQL dialect'leri
SUPPORTED_DIALECTS = [
    "default",
    "mysql",
    "sqlite",
    "postgres",
    "bigquery",
    "hive",
    "oracle",
    "redshift",
    "snowflake",
    "sparksql",
    "tsql",
]

# SQL Parse için Pydantic model
class SQLParseRequest(BaseModel):
    user_sql: str = Field(..., min_length=1, description="Parse edilecek SQL sorgusu.")

# SQL Manipulation için Pydantic model
class SQLManipulationRequest(BaseModel):
    user_sql: str = Field(..., min_length=1, description="Manipüle edilecek SQL sorgusu.")
    with_order: bool = Field(False, description="ORDER BY eklenip eklenmeyeceğini belirler.")
    limit: int = Field(10, ge=1, description="Sonuç setine eklenecek maksimum limit.")
    dialect: str = Field(
        "default",
        description="Hedef SQL dialect'i. Desteklenenler: mysql, postgres, oracle, tsql, sqlite, default.",
    )

# SQL Transpile için Pydantic model
class SQLTranspileRequest(BaseModel):
    user_sql: str = Field(..., min_length=1, description="Dönüştürülecek SQL sorgusu.")
    source_dialect: str = Field(..., description="Kaynak SQL dialect'i.")
    target_dialect: str = Field(..., description="Hedef SQL dialect'i.")

# SQL Validasyon için Pydantic model
class SQLValidationRequest(BaseModel):
    user_sql: str = Field(..., min_length=1, description="Validasyonu yapılacak SQL sorgusu.")

# SQL için Pydantic Model
class SQLColumnRequest(BaseModel):
    user_sql: str = Field(..., min_length=1, description="Sütunların alınacağı SQL sorgusu.")


# Parse Endpoint
@app.post("/sql/parse/")
async def parse_sql(request: SQLParseRequest):
    """
    SQL'i parse ederek AST (Abstract Syntax Tree) formatında döner.
    """
    try:
        parsed = parse_one(request.user_sql)
        return {"ast": repr(parsed)}
    except ParseError as e:
        raise HTTPException(status_code=400, detail=f"SQL parse edilemedi: {e}")

# Manipulate Endpoint
@app.post("/sql/manipulate/")
async def manipulate_sql(request: SQLManipulationRequest):
    """
    Kullanıcı tarafından sağlanan SQL sorgusuna LIMIT ekler. Eğer 'ORDER BY' mevcut değilse, varsayılan olarak ilk sütuna göre sıralama ekler.
    """
    try:
        # Gelen SQL'i parse et
        parsed = parse_one(request.user_sql)

        # with_order=True ise ORDER BY ekle (eğer yoksa)
        if request.with_order and not parsed.find(Order):  # ORDER BY var mı kontrol et
            try:
                parsed = parsed.order_by("1")  # Varsayılan olarak ilk sütuna göre sırala
            except Exception as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"'ORDER BY' eklenirken hata oluştu: {e}"
                )

        # LIMIT ekle
        try:
            parsed = parsed.limit(request.limit)
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"'LIMIT' eklenirken hata oluştu: {e}",
            )

        # Dialect kontrolü
        if request.dialect not in SUPPORTED_DIALECTS:
            raise HTTPException(
                status_code=400,
                detail=f"Geçersiz dialect '{request.dialect}'. Desteklenen dialect'ler: {', '.join(SUPPORTED_DIALECTS)}.",
            )

        # Manipüle edilmiş SQL'i döndür
        return {"manipulated_sql": parsed.sql(dialect=request.dialect)}

    except ParseError as e:
        raise HTTPException(
            status_code=400, detail=f"SQL parse edilemedi. Geçersiz sorgu: {e}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Beklenmeyen bir hata oluştu: {str(e)}",
        )

# Transpile Endpoint
@app.post("/sql/transpile/")
async def transpile_sql(request: SQLTranspileRequest):
    """
    SQL'i bir dialect'ten başka bir dialect'e dönüştürür.
    """
    if request.source_dialect not in SUPPORTED_DIALECTS or request.target_dialect not in SUPPORTED_DIALECTS:
        raise HTTPException(
            status_code=400,
            detail=f"Desteklenmeyen dialect. Desteklenen dialect'ler: {', '.join(SUPPORTED_DIALECTS)}"
        )

    try:
        transpiled_sql = transpile(
            request.user_sql,
            read=request.source_dialect,
            write=request.target_dialect
        )
        return {"transpiled_sql": transpiled_sql[0]}
    except ParseError as e:
        raise HTTPException(status_code=400, detail=f"SQL parse edilemedi: {e}")
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Beklenmeyen bir hata oluştu: {str(e)}",
        )

# Validation Endpoint
@app.post("/sql/validate/")
async def validate_sql(request: SQLValidationRequest):
    """
    SQL'in geçerli olup olmadığını kontrol eder.
    """
    try:
        parse_one(request.user_sql)
        return {"valid": True}
    except ParseError:
        return {"valid": False}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Beklenmeyen bir hata oluştu: {str(e)}",
        )

# Get Columns Endpoint
@app.post("/sql/columns/")
async def get_columns(request: SQLColumnRequest):
    """
    Verilen SQL sorgusundaki ana SELECT ifadesinin tüm sütunlarını döner.
    """
    try:
        # SQL sorgusunu parse et
        parsed = parse_one(request.user_sql)

        # Ana SELECT ifadesindeki tüm sütunları al
        columns = []
        for exp in parsed.selects:
            if hasattr(exp, "this"):
                columns.append(str(exp.this))  # Sütun ismini ekle

        return {"columns": columns}

    except ParseError as e:
        raise HTTPException(
            status_code=400,
            detail=f"SQL parse edilemedi: {e}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Beklenmeyen bir hata oluştu: {str(e)}"
        )