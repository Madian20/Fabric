## Project Idea

After studying for the **DP-600** (Fabric Analytics Engineer) and **DP-700** (Fabric Data Engineer) certifications, I wanted to apply what I learned on a real, end-to-end project instead of keeping it theoretical.

I chose the Olist E-Commerce dataset and built an end-to-end solution in Microsoft Fabric using the Medallion architecture, from raw data ingestion to a final Power BI report

---

## Project Architecture

<img width="2732" height="2060" alt="fabric_medallion_architecture_v5 (1)" src="https://github.com/user-attachments/assets/7490bf0a-f8e2-487d-a061-da380fe2a9f5" />

The project follows the **Medallion architecture** inside Microsoft Fabric:

- **Source:** ADLS Gen2 holding the raw Olist CSV files.
- **Bronze (Lakehouse):** Raw data only, exposed through a OneLake Shortcut with no copying or changes.
- **Silver (Lakehouse):** Cleaning and incremental load (watermark + merge). The data keeps the same shape as Bronze.
- **Gold (Warehouse):** Data is reshaped into fact and dimension tables. A semantic model is built on the Gold tables, and an ontology is built on top of the semantic model.
- **Power BI:** Reports built on the semantic model.
- **Cross-cutting:** Workspace Identity + OneLake security, and Git integration for version control of the whole project.

---

## Tools & Technologies

| Layer / Purpose | Tool |
|---|---|
| **Source storage** | Azure Data Lake Storage Gen2 (ADLS Gen2) |
| **Bronze** | Microsoft Fabric Lakehouse (OneLake Shortcut) |
| **Silver** | Microsoft Fabric Lakehouse, PySpark notebook, Delta tables |
| **Gold** | Microsoft Fabric Warehouse, PySpark notebook for  (loading & reshaping the data) |
| **Semantic layer** | Power BI semantic model (Direct Lake on OneLake), DAX |
| **Ontology** | Microsoft Fabric Ontology (built on top of the semantic model) |
| **Reporting** | Power BI |
| **Security** | Workspace Identity, OneLake security |
| **Version control** | GitHub, Fabric Git integration |
| **Project tracking** | GitHub Issues |

---

## Screenshots

### Lineage View

<img width="1755" height="623" alt="image" src="https://github.com/user-attachments/assets/87e36f2e-492a-47b9-9173-0899c24611d6" />

### Loading to Silver 

<img width="587" height="659" alt="Screenshot 2026-10-22 130530" src="https://github.com/user-attachments/assets/4fa440bb-ee56-4e7f-bc97-61e5b98a4ac2" />

### Semantic Model

<img width="1335" height="561" alt="Screenshot 2026-10-18 075004" src="https://github.com/user-attachments/assets/671e32d4-bfe1-4120-a16c-37d32cdbd871" />

### Ontology 

<img width="1377" height="577" alt="Screenshot 2026-10-20 084606" src="https://github.com/user-attachments/assets/5d44c812-9554-42a7-91e6-513da12e0cfa" />

### Overview Report

<img width="1432" height="814" alt="Screenshot 2026-10-21 154718" src="https://github.com/user-attachments/assets/add9eb66-2d69-44e7-97dd-0d4e4a1087e8" />

---

## Project Log

The project was tracked step by step using GitHub Issues, with each task and documentation item recorded in its own issue. For the full details of every stage, see the [closed project issues](https://github.com/Madian20/Fabric/issues?q=is%3Aissue+state%3Aclosed).
