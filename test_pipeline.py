import uuid
from pathlib import Path
from cua_orchestrator import run_parcel_pipeline

# paramètres de test
out_dir = Path("out_test")
out_dir.mkdir(parents=True, exist_ok=True)

result = run_parcel_pipeline(
    parcels="AC 0494",
    insee="33234",
    commune="Latresne",
    temp_dir=Path("out_test"),
    insee_csv_path="CONFIG/v_commune_2025.csv",
    mapping_path="CONFIG/mapping_layers.json",
    user_id="55c68f76-419b-4951-ba5c-6c9bfa202899"  # 👈 ton vrai UUID
)


print("Résultat :", result)
