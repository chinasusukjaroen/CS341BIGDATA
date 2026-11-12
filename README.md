# CS341BIGDATA
# [PM2.5 Trend and Weather Analysis: วิเคราะห์แนวโน้ม PM2.5 และสภาพอากาศ]

## สมาชิกในกลุ่ม

| ลำดับ | ชื่อ–นามสกุล | รหัสนักศึกษา |
|---|---|---|
| 1 | กันต์ธร ยืนยงชัยวัฒน์ | 6709420019 |
| 2 | ชินะสุ สุขเจริญ |  6709616459 |
| 3 | ธนปภพ ศรีสวัสดิ์ | 6709616525 |
| 4 | อิทธิพล จับชิ้น | 6709616970 |

## ภาพรวมโครงงาน

ระบบประมวลผลข้อมูลฝุ่น **PM2.5** จาก Air4Thai ร่วมกับข้อมูลสภาพอากาศย้อนหลังที่เรียกผ่าน **Open-Meteo Historical Weather API** เพื่อศึกษาการเปลี่ยนแปลงของฝุ่นรายปี ความสัมพันธ์กับความเร็วลมและปริมาณฝน


## วิธีติดตั้งและ Deploy

### 1. ดาวน์โหลด Repository ลง AWS (แนะนำให้รันบน AWS Cloudshell)

```bash
git clone <REPOSITORY_URL>
cd <REPOSITORY_FOLDER>
```

### 2. ตั้งค่า `.env` ที่ Root ของโปรเจกต์

อย่าลืม cp .env.example .env ก่อนทำการแก้ไข .env
```dotenv
AWS_REGION=us-east-1
S3_BUCKET=<YOUR_UNIQUE_S3_BUCKET_NAME>
GLUE_DATABASE=pm25_weather
ICEBERG_TABLE=pm25_weather_iceberg
IAM_ROLE=LabRole
GLUE_JOB=pm25-iceberg-full-year-publish
STATE_MACHINE_NAME=pm25-weather-pipeline
TRIGGER=both
```

`TRIGGER=both` เปิดการตั้งค่า EventBridge สำหรับ S3 และรองรับการทดลองเรียก State Machine ด้วยตนเองด้วย ส่วน `TRIGGER=manual` ไม่ตั้งค่า S3 EventBridge Trigger

### 4. ตรวจสอบโครงสร้างไฟล์

Deployment Script ที่พัฒนาสำหรับโปรเจกต์นี้คาดหวังไฟล์สำคัญ เช่น

```text
.
├── deploy.sh
├── .env
├── aws/
│   ├── lambdas/
│   │   ├── check_batch/app.py
│   │   ├── validate_batch/app.py
│   │   ├── fetch_weather/app.py
│   │   ├── validate_weather/app.py
│   │   ├── transform_batch/app.py
│   │   ├── verify_batch/app.py
│   │   ├── finalize_iceberg/app.py
│   │   ├── analysis_batch/app.py
│   │   └── quarantine_batch/app.py
│   └── glue/
├── sql/
└── test/
    └── common.sh
```

> โครงสร้างข้างต้นอ้างอิง Deployment Script ที่ใช้พัฒนา หาก Repository ปัจจุบันมีการย้ายไฟล์ ต้องปรับคำสั่งให้ตรงกับไฟล์จริง

### 5. Deploy

```bash
bash deploy.sh
```

รอให้ Deploy สำเร็จ จากนั้นตรวจสอบ Resources ที่สร้าง

## วิธีทดลองรัน (End-to-End)

### ใช้ Test Scripts

โปรเจกต์มีชุดสคริปต์ทดสอบใน `test/` และฟังก์ชันร่วมใน `test/common.sh` สำหรับดาวน์โหลดและอัปโหลดข้อมูล โดยให้ตรวจสอบชื่อไฟล์ใน Repository ก่อนรัน:

```bash
ls -la test/
```

จากนั้นเรียกสคริปต์ที่มีอยู่จริง เช่น:

```bash
bash test/<TEST_SCRIPT_NAME>.sh
```

Test Cases ที่วางแผนไว้ประกอบด้วยข้อมูลปี 2024/2026, ข้อมูลที่ดัดแปลง, การตรวจจับ Batch ซ้ำ และกรณีข้อมูลไม่ถูกต้อง

### วิธีตรวจสอบผลลัพธ์

```bash
# ดูข้อมูลที่อัปโหลด
aws s3 ls s3://<YOUR_UNIQUE_S3_BUCKET_NAME>/incoming/pm25/ --recursive

# ดูผลวิเคราะห์ (เมื่อ Pipeline ทำงานสำเร็จ)
aws s3 ls s3://<YOUR_UNIQUE_S3_BUCKET_NAME>/analytics/ --recursive

# ดูข้อมูล Quarantine
aws s3 ls s3://<YOUR_UNIQUE_S3_BUCKET_NAME>/quarantine/pm25/ --recursive

# ตรวจสอบ Glue Tables
aws glue get-tables --database-name pm25_weather --region us-east-1 \
  --query 'TableList[].Name' --output table
```

สามารถใช้ **Amazon Athena** Query ตาราง `pm25_weather.pm25_weather_iceberg` หลังจาก Glue Publish สำเร็จ โดยเลือก WorkGroup และ S3 Query Result Location ที่ตั้งค่าไว้

## แหล่งข้อมูล

- [Air4Thai](https://air4thai.pcd.go.th/)
- [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)
