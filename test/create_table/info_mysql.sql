SHOW DATABASES;
CREATE DATABASE patients;
CREATE TABLE patients.info (
    patient_id           BIGINT PRIMARY KEY COMMENT 'Mã định danh duy nhất của bệnh nhân',
    national_id          VARCHAR(20)  NOT NULL COMMENT 'Số định danh cá nhân / CCCD của bệnh nhân',
    full_name            VARCHAR(255) NOT NULL COMMENT 'Họ và tên đầy đủ của bệnh nhân',
    age                  INT          COMMENT 'Tuổi của bệnh nhân tại thời điểm cập nhật gần nhất',
    gender               ENUM('Nam', 'Nữ', 'Khác') COMMENT 'Giới tính của bệnh nhân',
    date_of_birth        VARCHAR(20)  COMMENT 'Ngày sinh (định dạng YYYY-MM-DD)',
    address              VARCHAR(500) COMMENT 'Địa chỉ cư trú hiện tại của bệnh nhân',
    health_insurance_id  VARCHAR(30)  COMMENT 'Mã thẻ bảo hiểm y tế',
    phone                VARCHAR(20)  COMMENT 'Số điện thoại liên hệ của bệnh nhân',
    occupation           VARCHAR(255) COMMENT 'Nghề nghiệp hiện tại của bệnh nhân',
    height_cm            DECIMAL(5,2) COMMENT 'Chiều cao của bệnh nhân (cm)',
    weight_kg            DECIMAL(5,2) COMMENT 'Cân nặng của bệnh nhân (kg)',
    bmi                  DECIMAL(4,1) COMMENT 'Chỉ số BMI = cân nặng / (chiều cao(m))^2',
    blood_type           VARCHAR(5)   COMMENT 'Nhóm máu (A, B, AB, O, Rh+/Rh-)',
    comorbidities        VARCHAR(500) COMMENT 'Các bệnh nền hoặc tình trạng sức khỏe',
    
    created_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                        COMMENT 'Thời điểm bản ghi được tạo',
    
    updated_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                        ON UPDATE CURRENT_TIMESTAMP
                        COMMENT 'Thời điểm bản ghi được cập nhật gần nhất'
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COMMENT='Bảng lưu trữ hồ sơ y tế của bệnh nhân phục vụ phân tích và quản lý hồ sơ sức khỏe điện tử';


DROP TABLE patients.info;


