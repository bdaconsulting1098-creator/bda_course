# Databricks notebook source
# MAGIC %md
# MAGIC # Gemini 微调教学脚本
# MAGIC **前提：** `train.jsonl` 已按 Vertex AI 格式准备好，无需转换。

# COMMAND ----------

# %pip install google-cloud-aiplatform google-cloud-storage 
# dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 第 1 步：配置参数（根据你的项目修改）

# COMMAND ----------

PROJECT_ID = ""
BUCKET_NAME = "aw-sql-tuning-bucket-turnkey-brook"
LOCATION = "us-central1"
# --- 动态获取 Databricks 当前用户名 ---
# Databricks 中当前用户的邮箱通常会包含在路径中
# 使用 dbutils 获取用户名（如果在 notebook 内）或通过 Spark
try:
    username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
except Exception:
    # 备用方法：从 Spark 配置获取，或者手动设置
    username = spark.sparkContext.getConf().get("spark.databricks.userName", "unknown")

# 动态构建服务账号密钥文件路径
SERVICE_ACCOUNT_PATH = f"/Workspace/Users/{username}/AI/ft-secret.json"


# 训练数据（本地文件，已为正确格式）
TRAINING_FILE = "train.jsonl"

# 微调参数
TUNED_MODEL_NAME = "my_aw_sql_model"
EPOCHS = 4
ADAPTER_SIZE = 16
LEARNING_RATE_MULTIPLIER = 0.3

# 测试用问题
TEST_PROMPT = "Retrieve the name and list price of all products"

print("✓ 参数配置完成")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 第 2 步：使用服务账号认证

# COMMAND ----------

import vertexai
from google.oauth2 import service_account

print("🔑 开始认证...")
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_PATH,
    scopes=['https://www.googleapis.com/auth/cloud-platform']
)
vertexai.init(project=PROJECT_ID, location=LOCATION, credentials=credentials)
print("✓ 认证成功")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 第 3 步：创建或获取 Cloud Storage 存储桶

# COMMAND ----------

from google.cloud import storage

print("☁️ 准备存储桶...")
storage_client = storage.Client(project=PROJECT_ID, credentials=credentials)
try:
    bucket = storage_client.create_bucket(BUCKET_NAME, location=LOCATION)
    print(f"✓ 存储桶创建成功: {BUCKET_NAME}")
except Exception as e:
    if "Already exists" in str(e) or "409" in str(e):
        bucket = storage_client.bucket(BUCKET_NAME)
        print(f"✓ 存储桶已存在: {BUCKET_NAME}")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 第 4 步：上传训练数据到 GCS

# COMMAND ----------

print("📤 上传训练数据...")
blob = bucket.blob("train.jsonl")
blob.upload_from_filename(TRAINING_FILE)
train_gcs_path = f"gs://{BUCKET_NAME}/train.jsonl"
print(f"✓ 上传完成: {train_gcs_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 第 5 步：启动监督微调任务

# COMMAND ----------

from vertexai.tuning import sft

print("🚀 启动微调任务...")
print(f"   基础模型: gemini-2.5-flash")
print(f"   训练数据: {train_gcs_path}")
print(f"   迭代轮次: {EPOCHS}")

sft_job = sft.train(
    source_model="gemini-2.5-flash",
    train_dataset=train_gcs_path,
    tuned_model_display_name=TUNED_MODEL_NAME,
    epochs=EPOCHS,
    adapter_size=ADAPTER_SIZE,
    learning_rate_multiplier=LEARNING_RATE_MULTIPLIER,
)

print(f"✓ 任务已创建，ID: {sft_job.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 第 6 步：监控任务状态，直到完成

# COMMAND ----------

import time

print("⏳ 等待任务完成（每 60 秒检查一次）...")
while not sft_job.has_ended:
    time.sleep(60)
    sft_job.refresh()
    print(f"   当前状态: {sft_job.state.name}")

if sft_job.state.name == "JOB_STATE_SUCCEEDED":
    print(f"✅ 微调成功！")
    print(f"   模型名称: {sft_job.tuned_model_name}")
    print(f"   端点名称: {sft_job.tuned_model_endpoint_name}")
else:
    print(f"❌ 任务失败: {sft_job.state.name}")
    raise RuntimeError(f"Fine-tuning job failed with state: {sft_job.state.name}")

# COMMAND ----------

sft_job.tuned_model_endpoint_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## 第 7 步：使用微调后的模型进行测试推理

# COMMAND ----------

# DBTITLE 1,Cell 16
import requests
import json
from google.auth.transport import requests as auth_requests

print("🤖 开始测试...")
TEST_PROMPT = "top 10 customers in ptofits"
# Refresh credentials to get a valid token
request = auth_requests.Request()
credentials.refresh(request)

# Build the endpoint URL
url = f"https://{LOCATION}-aiplatform.googleapis.com/v1/{sft_job.tuned_model_endpoint_name}:generateContent"

headers = {
    "Authorization": f"Bearer {credentials.token}",
    "Content-Type": "application/json"
}

# Format request in Gemini API format
payload = {
    "contents": [{
        "role": "user",
        "parts": [{"text": TEST_PROMPT}]
    }]
}

# Make the request
response = requests.post(url, headers=headers, json=payload)

if response.status_code == 200:
    result = response.json()
    output_text = result['candidates'][0]['content']['parts'][0]['text']
    print(f"   输入: {TEST_PROMPT}")
    print(f"   输出: {output_text}")
else:
    print(f"❌ 请求失败: {response.status_code}")
    print(f"   错误: {response.text}")

# COMMAND ----------

import time
from vertexai.generative_models import GenerativeModel

# 1. 定义指令（可统一添加到每个问题前）
INSTRUCTION = "Table names must include schema. Return SQL only, no explanation.\n\n"

# 2. 测试问题列表（注意逗号）
raw_questions = [
    "Retrieve the name and list price of all products",
    "Count the number of products in each product subcategory",
    "Show total sales for July 2003"
]

# 把指令加进每个问题
test_questions = [INSTRUCTION + q for q in raw_questions]

print("===== 基础模型（未微调）=====")
base_model = GenerativeModel("gemini-2.5-flash")
for q in test_questions:
    resp = base_model.generate_content(q)
    print(f"Q: {q}\nA: {resp.text}\n")

print("===== 微调模型 =====")


try:
    # 重要：请使用 sft_job.tuned_model_name（模型资源名），而非 endpoints/...
    # 如果你的训练脚本还在内存里，直接用变量：
    # ft_model = GenerativeModel(sft_job.tuned_model_name)
    #
    # 如果已经丢失变量，可用以下硬编码（但请替换成你实际的模型资源名）：
    ft_model = GenerativeModel(f"{sft_job.tuned_model_endpoint_name}")
    
    for q in test_questions:
        resp = ft_model.generate_content(q)
        print(f"Q: {q}\nA: {resp.text}\n")
except Exception as e:
    print(f"微调模型调用失败: {e}")
    print("提示：请确认模型已部署完成，或改用基础模型 + Few-shot 演示。")

# COMMAND ----------

import time
from vertexai.generative_models import GenerativeModel

# ==================== 1. 构建完整 Schema 描述（RAG 核心） ====================
# 把你 AdventureWorks 所有表及列写在这里，大模型就能“看到”所有表结构
SCHEMA = """
Database: AdventureWorks

Tables and columns:
- Production.Product: ProductID, Name, ProductNumber, Color, StandardCost, ListPrice, Size, Weight, ProductSubcategoryID, ProductModelID, SellStartDate, SellEndDate, DiscontinuedDate, rowguid, ModifiedDate
- Production.ProductSubcategory: ProductSubcategoryID, ProductCategoryID, Name, rowguid, ModifiedDate
- Production.ProductCategory: ProductCategoryID, Name, rowguid, ModifiedDate
- Production.ProductModel: ProductModelID, Name, CatalogDescription, Instructions, rowguid, ModifiedDate
- Production.ProductInventory: ProductID, LocationID, Shelf, Bin, Quantity, rowguid, ModifiedDate
- Production.Location: LocationID, Name, CostRate, Availability, ModifiedDate
- Production.ProductReview: ProductReviewID, ProductID, ReviewerName, ReviewDate, EmailAddress, Rating, Comments, ModifiedDate
- Production.ProductDescription: ProductDescriptionID, Description, rowguid, ModifiedDate
- Production.ProductModelProductDescriptionCulture: ProductModelID, ProductDescriptionID, CultureID, ModifiedDate
- Production.BillOfMaterials: BillOfMaterialsID, ProductAssemblyID, ComponentID, StartDate, EndDate, UnitMeasureCode, BOMLevel, PerAssemblyQty, ModifiedDate
- Production.UnitMeasure: UnitMeasureCode, Name, ModifiedDate
- Production.WorkOrder: WorkOrderID, ProductID, OrderQty, StockedQty, ScrappedQty, StartDate, EndDate, DueDate, ScrapReasonID, ModifiedDate
- Production.WorkOrderRouting: WorkOrderID, ProductID, OperationSequence, LocationID, ScheduledStartDate, ScheduledEndDate, ActualStartDate, ActualEndDate, ActualResourceHrs, PlannedCost, ActualCost, ModifiedDate
- Production.ScrapReason: ScrapReasonID, Name, ModifiedDate
- Production.Culture: CultureID, Name, ModifiedDate
- Production.Document: DocumentID, Title, FileName, FileExtension, Revision, ChangeNumber, Status, DocumentSummary, Document, ProductID, ModifiedDate
- Production.Illustration: IllustrationID, Diagram, ModifiedDate
- Production.ProductCostHistory: ProductID, StartDate, EndDate, StandardCost, ModifiedDate
- Production.ProductListPriceHistory: ProductID, StartDate, EndDate, ListPrice, ModifiedDate
- Production.TransactionHistory: TransactionID, ProductID, ReferenceOrderID, ReferenceOrderLineID, TransactionDate, TransactionType, Quantity, ActualCost, ModifiedDate
- Production.TransactionHistoryArchive: TransactionID, ProductID, ReferenceOrderID, ReferenceOrderLineID, TransactionDate, TransactionType, Quantity, ActualCost, ModifiedDate

- Sales.SalesOrderHeader: SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate, Status, OnlineOrderFlag, SalesOrderNumber, PurchaseOrderNumber, AccountNumber, CustomerID, SalesPersonID, TerritoryID, BillToAddressID, ShipToAddressID, ShipMethodID, CreditCardID, CreditCardApprovalCode, CurrencyRateID, SubTotal, TaxAmt, Freight, TotalDue, Comment, rowguid, ModifiedDate
- Sales.SalesOrderDetail: SalesOrderID, SalesOrderDetailID, CarrierTrackingNumber, OrderQty, ProductID, SpecialOfferID, UnitPrice, UnitPriceDiscount, LineTotal, rowguid, ModifiedDate
- Sales.Customer: CustomerID, PersonID, StoreID, TerritoryID, AccountNumber, rowguid, ModifiedDate
- Sales.SalesPerson: BusinessEntityID, TerritoryID, SalesQuota, Bonus, CommissionPct, SalesYTD, SalesLastYear, rowguid, ModifiedDate
- Sales.SalesTerritory: TerritoryID, Name, CountryRegionCode, Group, SalesYTD, SalesLastYear, CostYTD, CostLastYear, rowguid, ModifiedDate
- Sales.SpecialOffer: SpecialOfferID, Description, DiscountPct, Type, Category, StartDate, EndDate, MinQty, MaxQty, rowguid, ModifiedDate
- Sales.SpecialOfferProduct: SpecialOfferID, ProductID, rowguid, ModifiedDate
- Sales.CreditCard: CreditCardID, CardType, CardNumber, ExpMonth, ExpYear, ModifiedDate
- Sales.CurrencyRate: CurrencyRateID, CurrencyRateDate, FromCurrencyCode, ToCurrencyCode, AverageRate, EndOfDayRate, ModifiedDate
- Sales.ShoppingCartItem: ShoppingCartItemID, ShoppingCartID, Quantity, ProductID, DateCreated, ModifiedDate
- Sales.SalesReason: SalesReasonID, Name, ReasonType, ModifiedDate
- Sales.SalesOrderHeaderSalesReason: SalesOrderID, SalesReasonID, ModifiedDate

- Person.Person: BusinessEntityID, PersonType, NameStyle, Title, FirstName, MiddleName, LastName, Suffix, EmailPromotion, AdditionalContactInfo, Demographics, rowguid, ModifiedDate
- Person.BusinessEntity: BusinessEntityID, rowguid, ModifiedDate
- Person.EmailAddress: BusinessEntityID, EmailAddressID, EmailAddress, rowguid, ModifiedDate
- Person.PersonPhone: BusinessEntityID, PhoneNumber, PhoneNumberTypeID, ModifiedDate
- Person.Address: AddressID, AddressLine1, AddressLine2, City, StateProvinceID, PostalCode, rowguid, ModifiedDate
- Person.BusinessEntityAddress: BusinessEntityID, AddressID, AddressTypeID, rowguid, ModifiedDate
- Person.StateProvince: StateProvinceID, StateProvinceCode, CountryRegionCode, IsOnlyStateProvinceFlag, Name, TerritoryID, rowguid, ModifiedDate
- Person.CountryRegion: CountryRegionCode, Name, ModifiedDate
- Person.AddressType: AddressTypeID, Name, rowguid, ModifiedDate
- Person.PhoneNumberType: PhoneNumberTypeID, Name, ModifiedDate
- Person.ContactType: ContactTypeID, Name, ModifiedDate

- HumanResources.Employee: BusinessEntityID, NationalIDNumber, LoginID, OrganizationNode, OrganizationLevel, JobTitle, BirthDate, MaritalStatus, Gender, HireDate, SalariedFlag, VacationHours, SickLeaveHours, CurrentFlag, rowguid, ModifiedDate
- HumanResources.Department: DepartmentID, Name, GroupName, ModifiedDate
- HumanResources.EmployeeDepartmentHistory: BusinessEntityID, DepartmentID, ShiftID, StartDate, EndDate, ModifiedDate
- HumanResources.Shift: ShiftID, Name, StartTime, EndTime, ModifiedDate
- HumanResources.JobCandidate: JobCandidateID, BusinessEntityID, Resume, ModifiedDate

- Purchasing.PurchaseOrderHeader: PurchaseOrderID, RevisionNumber, Status, EmployeeID, VendorID, ShipMethodID, OrderDate, ShipDate, SubTotal, TaxAmt, Freight, TotalDue, ModifiedDate
- Purchasing.PurchaseOrderDetail: PurchaseOrderID, PurchaseOrderDetailID, DueDate, OrderQty, ProductID, UnitPrice, LineTotal, ReceivedQty, RejectedQty, StockedQty, ModifiedDate
- Purchasing.Vendor: BusinessEntityID, AccountNumber, Name, CreditRating, PreferredVendorStatus, ActiveFlag, PurchasingWebServiceURL, ModifiedDate
- Purchasing.ProductVendor: ProductID, BusinessEntityID, AverageLeadTime, StandardPrice, LastReceiptCost, LastReceiptDate, MinOrderQty, MaxOrderQty, OnOrderQty, UnitMeasureCode, ModifiedDate
- Purchasing.ShipMethod: ShipMethodID, Name, ShipBase, ShipRate, rowguid, ModifiedDate
"""

# ==================== 2. 构建 RAG 风格的 prompt 模板 ====================
def build_prompt(question):
    return f"""You are a SQL expert. Use only the tables/columns provided below. All table names must include schema (e.g., Production.Product). Return SQL only, no explanation.

{SCHEMA}

Question: {question}
SQL:"""

# ==================== 3. 测试问题 ====================
raw_questions = [
"top 10 customers in ptofit, sales"
]

print("===== 基础模型 + Schema Prompt (RAG) =====")
base_model = GenerativeModel("gemini-2.5-flash")
for q in raw_questions:
    prompt = build_prompt(q)
    resp = base_model.generate_content(prompt)
    print(f"Q: {q}\nA: {resp.text}\n")

# ==================== 4. 微调模型（如果可用） ====================
print("===== 微调模型 =====")


try:
    # 使用模型资源名，不是端点名！
    # 如果 sft_job 变量还在，直接用 sft_job.tuned_model_name
    ft_model = GenerativeModel(sft_job.tuned_model_name)
    ft_model = GenerativeModel(f"{sft_job.tuned_model_endpoint_name}")
    for q in raw_questions:
        prompt = build_prompt(q)   # 同样带上 schema 保证效果
        resp = ft_model.generate_content(prompt)
        print(f"Q: {q}\nA: {resp.text}\n")
except NameError:
    print("sft_job 变量不存在，跳过微调模型测试。")
except Exception as e:
    print(f"微调模型调用失败: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## （可选）清理：删除端点以节省资源

# COMMAND ----------

# 取消注释以下代码以删除端点
# from google.cloud import aiplatform
# print("🧹 清理端点...")
# aiplatform.init(project=PROJECT_ID, location=LOCATION)
# endpoint = aiplatform.Endpoint(sft_job.tuned_model_endpoint_name)
# endpoint.delete()
# print(f"✓ 已删除端点: {sft_job.tuned_model_endpoint_name}")

print("（跳过清理步骤 — 取消注释上方代码以删除端点）")
