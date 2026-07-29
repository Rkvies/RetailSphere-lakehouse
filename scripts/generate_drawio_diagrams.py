"""
Generates .drawio (mxGraph XML) files from a simple node/edge spec.
Run once locally to produce docs/diagrams/*.drawio - not part of the
pipeline itself, a one-time documentation-generation utility.
"""
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom

def build_drawio(filename, nodes, edges, title):
    """
    nodes: list of (id, label, x, y, w, h, fill_color)
    edges: list of (source_id, target_id, label)
    """
    mxfile = ET.Element("mxfile", host="app.diagrams.net")
    diagram = ET.SubElement(mxfile, "diagram", name=title, id="diagram1")
    model = ET.SubElement(diagram, "mxGraphModel", dx="800", dy="600", grid="1",
                           gridSize="10", guides="1", tooltips="1", connect="1",
                           arrows="1", fold="1", page="1", pageScale="1",
                           pageWidth="1200", pageHeight="800", math="0", shadow="0")
    root = ET.SubElement(model, "root")
    ET.SubElement(root, "mxCell", id="0")
    ET.SubElement(root, "mxCell", id="1", parent="0")

    for node_id, label, x, y, w, h, color in nodes:
        cell = ET.SubElement(root, "mxCell", id=node_id,
                              value=label,
                              style=f"rounded=1;whiteSpace=wrap;html=1;fillColor={color};fontSize=12;",
                              vertex="1", parent="1")
        ET.SubElement(cell, "mxGeometry", x=str(x), y=str(y), width=str(w), height=str(h), **{"as": "geometry"})

    for i, (src, tgt, label) in enumerate(edges):
        edge = ET.SubElement(root, "mxCell", id=f"e{i}",
                              value=label,
                              style="edgeStyle=orthogonalEdgeStyle;rounded=0;html=1;fontSize=11;",
                              edge="1", parent="1", source=src, target=tgt)
        ET.SubElement(edge, "mxGeometry", relative="1", **{"as": "geometry"})

    xml_str = ET.tostring(mxfile, encoding="unicode")
    pretty = minidom.parseString(xml_str).toprettyxml(indent="  ")
    with open(filename, "w") as f:
        f.write(pretty)
    print(f"Wrote {filename}")


# ---------------------------------------------------------------------
# 1. Medallion Architecture
# ---------------------------------------------------------------------
build_drawio(
    "docs/diagrams/01_medallion_architecture.drawio",
    nodes=[
        ("src", "Source Systems\n(Customer, Product,\nSales, etc.)", 40, 140, 160, 80, "#DAE8FC"),
        ("bronze", "BRONZE\nRaw, append-only\n+ metadata", 260, 140, 160, 80, "#F8CECC"),
        ("silver", "SILVER\nDeduplicated, validated\nSCD Type 2", 480, 140, 160, 80, "#D5E8D4"),
        ("gold", "GOLD\nStar schema\nAnalytics/ML-ready", 700, 140, 160, 80, "#FFE6CC"),
        ("bi", "BI Tools /\nPower BI", 920, 80, 140, 60, "#E1D5E7"),
        ("ml", "ML Notebooks", 920, 200, 140, 60, "#E1D5E7"),
    ],
    edges=[
        ("src", "bronze", ""), ("bronze", "silver", ""), ("silver", "gold", ""),
        ("gold", "bi", ""), ("gold", "ml", ""),
    ],
    title="Medallion Architecture",
)

# ---------------------------------------------------------------------
# 2. High-Level Architecture
# ---------------------------------------------------------------------
build_drawio(
    "docs/diagrams/02_high_level_architecture.drawio",
    nodes=[
        ("landing", "Landing Zone\n(daily source files)", 40, 300, 160, 70, "#DAE8FC"),
        ("ingest", "Ingestion Layer\n(config-driven\nBronze loader)", 260, 300, 160, 70, "#F8CECC"),
        ("bronze", "Bronze Delta\nTables", 480, 200, 140, 60, "#F8CECC"),
        ("silver", "Silver Delta\nTables", 480, 300, 140, 60, "#D5E8D4"),
        ("gold", "Gold Delta\nTables", 480, 400, 140, 60, "#FFE6CC"),
        ("processing", "Processing Layer\n(PySpark / Spark SQL)", 700, 300, 160, 70, "#D5E8D4"),
        ("orch", "Orchestration\n(Airflow DAGs)", 260, 480, 160, 60, "#E1D5E7"),
        ("gov", "Governance:\nLogging, DQ,\nException Handling", 40, 480, 160, 70, "#FFF2CC"),
        ("consume", "Consumption:\nPower BI, ML,\nDuckDB/Postgres", 920, 300, 160, 70, "#E1D5E7"),
    ],
    edges=[
        ("landing", "ingest", ""), ("ingest", "bronze", ""),
        ("bronze", "processing", ""), ("processing", "silver", ""),
        ("silver", "processing", ""), ("processing", "gold", ""),
        ("gold", "consume", ""),
        ("orch", "ingest", "triggers"), ("orch", "processing", "triggers"),
        ("gov", "ingest", "wraps"), ("gov", "processing", "wraps"),
    ],
    title="High-Level Architecture",
)

# ---------------------------------------------------------------------
# 3. Low-Level Architecture (Bronze -> Silver -> Gold detail)
# ---------------------------------------------------------------------
build_drawio(
    "docs/diagrams/03_low_level_architecture.drawio",
    nodes=[
        ("b1", "Read raw file", 40, 40, 140, 50, "#F8CECC"),
        ("b2", "Attach metadata\n(_source_file,\n_ingest_ts, _batch_id)", 200, 40, 160, 60, "#F8CECC"),
        ("b3", "Schema validation\n(reject to quarantine)", 380, 40, 160, 60, "#F8CECC"),
        ("b4", "Bronze Delta Table\n(partitioned by\n_ingest_date)", 560, 40, 160, 60, "#F8CECC"),

        ("s1", "Read Bronze\nincremental", 40, 160, 140, 50, "#D5E8D4"),
        ("s2", "Deduplicate\non business key", 200, 160, 160, 50, "#D5E8D4"),
        ("s3", "Apply DQ rules", 380, 160, 140, 50, "#D5E8D4"),
        ("s4", "Fact: incremental\nmerge / SCD1", 560, 130, 150, 50, "#D5E8D4"),
        ("s5", "Dimension: SCD2\nmerge", 560, 190, 150, 50, "#D5E8D4"),
        ("s6", "Silver Delta Table", 750, 160, 150, 50, "#D5E8D4"),

        ("g1", "Read Silver\nfacts + dims", 40, 280, 140, 50, "#FFE6CC"),
        ("g2", "Point-in-time join\n(dims + facts)", 200, 280, 160, 50, "#FFE6CC"),
        ("g3", "Build star schema", 400, 280, 160, 50, "#FFE6CC"),
        ("g4", "Gold Delta Table\n(OPTIMIZE ZORDER)", 600, 280, 160, 50, "#FFE6CC"),
    ],
    edges=[
        ("b1", "b2", ""), ("b2", "b3", ""), ("b3", "b4", ""),
        ("b4", "s1", ""),
        ("s1", "s2", ""), ("s2", "s3", ""),
        ("s3", "s4", "fact"), ("s3", "s5", "dimension"),
        ("s4", "s6", ""), ("s5", "s6", ""),
        ("s6", "g1", ""),
        ("g1", "g2", ""), ("g2", "g3", ""), ("g3", "g4", ""),
    ],
    title="Low-Level Architecture",
)

# ---------------------------------------------------------------------
# 4. Component Diagram
# ---------------------------------------------------------------------
build_drawio(
    "docs/diagrams/04_component_diagram.drawio",
    nodes=[
        ("cfg1", "table_config.yaml", 40, 40, 160, 50, "#FFF2CC"),
        ("cfg2", "env_config.yaml", 40, 110, 160, 50, "#FFF2CC"),
        ("log", "Logger Module", 260, 40, 150, 45, "#D5E8D4"),
        ("dq", "Data Quality Module", 260, 100, 150, 45, "#D5E8D4"),
        ("exc", "Exception Handler", 260, 160, 150, 45, "#D5E8D4"),
        ("util", "Delta Utils", 260, 220, 150, 45, "#D5E8D4"),
        ("brz", "bronze_loader.py", 480, 60, 150, 45, "#F8CECC"),
        ("slv", "silver_processor.py\nscd2_handler.py", 480, 130, 150, 55, "#F8CECC"),
        ("gld", "gold_builder.py", 480, 210, 150, 45, "#FFE6CC"),
        ("dag", "Airflow DAG", 680, 130, 150, 50, "#E1D5E7"),
    ],
    edges=[
        ("cfg1", "brz", ""), ("cfg1", "slv", ""), ("cfg1", "gld", ""),
        ("cfg2", "brz", ""),
        ("log", "brz", ""), ("log", "slv", ""), ("log", "gld", ""),
        ("dq", "brz", ""), ("dq", "slv", ""),
        ("exc", "brz", ""),
        ("util", "slv", ""), ("util", "gld", ""),
        ("dag", "brz", ""), ("dag", "slv", ""), ("dag", "gld", ""),
    ],
    title="Component Diagram",
)

# ---------------------------------------------------------------------
# 5. Sequence-like flow (drawn as a flowchart since drawio sequence
# diagrams are a different shape library - flowchart form preserves
# the branch logic which is the important part)
# ---------------------------------------------------------------------
build_drawio(
    "docs/diagrams/05_pipeline_run_flow.drawio",
    nodes=[
        ("trig", "Airflow: trigger\nrun_date", 40, 40, 150, 50, "#E1D5E7"),
        ("read", "Bronze: read\nraw file", 220, 40, 150, 50, "#F8CECC"),
        ("val", "Data Quality:\nvalidate schema", 400, 40, 150, 50, "#D5E8D4"),
        ("pass", "Valid rows ->\nwrite to Bronze", 600, 0, 160, 50, "#D5E8D4"),
        ("fail", "Invalid rows ->\nwrite to Quarantine", 600, 90, 170, 50, "#F8CECC"),
        ("logok", "Log: success\n(row_count)", 800, 0, 150, 50, "#FFF2CC"),
        ("logwarn", "Log: warning\n(rejected_count)", 800, 90, 160, 50, "#FFF2CC"),
        ("done", "Report status\nto Airflow", 1000, 40, 150, 50, "#E1D5E7"),
    ],
    edges=[
        ("trig", "read", ""), ("read", "val", ""),
        ("val", "pass", "valid"), ("val", "fail", "invalid"),
        ("pass", "logok", ""), ("fail", "logwarn", ""),
        ("logok", "done", ""), ("logwarn", "done", ""),
    ],
    title="Pipeline Run Flow",
)

# ---------------------------------------------------------------------
# 6. Deployment Mapping
# ---------------------------------------------------------------------
build_drawio(
    "docs/diagrams/06_deployment_mapping.drawio",
    nodes=[
        ("vscode", "VS Code /\nLocal Dev", 40, 40, 150, 50, "#DAE8FC"),
        ("dbce", "Databricks\nCommunity Edition", 40, 120, 150, 50, "#DAE8FC"),
        ("gh", "GitHub Repo", 40, 200, 150, 50, "#DAE8FC"),
        ("ga", "GitHub Actions\n(CI)", 40, 280, 150, 50, "#DAE8FC"),

        ("s3", "S3 / ADLS\nData Lake", 400, 40, 150, 50, "#D5E8D4"),
        ("emr", "EMR / Databricks\nJob Clusters", 400, 120, 150, 50, "#D5E8D4"),
        ("mwaa", "Managed Airflow\n(MWAA)", 400, 200, 150, 50, "#D5E8D4"),
        ("cicd", "Jenkins / GitHub\nActions CI-CD", 400, 280, 150, 50, "#D5E8D4"),
    ],
    edges=[
        ("vscode", "s3", "maps to"),
        ("dbce", "emr", "maps to"),
        ("gh", "mwaa", "maps to"),
        ("ga", "cicd", "maps to"),
    ],
    title="Deployment Mapping (Free Tier -> Production)",
)

# ---------------------------------------------------------------------
# 7. Star Schema ER Diagram (simplified box form)
# ---------------------------------------------------------------------
build_drawio(
    "docs/diagrams/07_star_schema_erd.drawio",
    nodes=[
        ("dim_cust", "dim_customer\n(SCD2)\ncustomer_sk PK\ncustomer_id\ncountry, segment\nis_current", 40, 180, 180, 100, "#D5E8D4"),
        ("dim_prod", "dim_product\n(SCD2)\nproduct_sk PK\nstock_code\nunit_price\nis_current", 40, 40, 180, 100, "#D5E8D4"),
        ("dim_store", "dim_store\n(SCD1)\nstore_sk PK\nstore_id, region", 40, 320, 180, 80, "#D5E8D4"),
        ("fact_sales", "fact_sales\ninvoice_id\ncustomer_sk FK\nproduct_sk FK\nstore_sk FK\nsale_date\nquantity, line_total", 320, 160, 200, 130, "#FFE6CC"),
        ("dim_date", "dim_date\ndate_sk PK\nfull_date, year, month", 320, 40, 180, 80, "#D5E8D4"),
        ("fact_ret", "fact_returns\nreturn_invoice_id\noriginal_invoice_id FK\nproduct_sk FK", 620, 100, 190, 80, "#FFE6CC"),
        ("fact_inv", "fact_inventory_snapshot\nstore_sk FK, product_sk FK\nsnapshot_date\nstock_on_hand", 620, 220, 210, 90, "#FFE6CC"),
    ],
    edges=[
        ("dim_cust", "fact_sales", "1:N"), ("dim_prod", "fact_sales", "1:N"),
        ("dim_store", "fact_sales", "1:N"), ("dim_date", "fact_sales", "1:N"),
        ("fact_sales", "fact_ret", "1:N"),
        ("dim_store", "fact_inv", "1:N"), ("dim_prod", "fact_inv", "1:N"),
    ],
    title="Star Schema ERD",
)

print("\nAll .drawio files generated in docs/diagrams/")
