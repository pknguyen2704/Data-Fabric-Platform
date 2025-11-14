CREATE SCHEMA iceberg.bronze
WITH (location = 'hdfs://namenode:9000/user/nifi/iceberg/bronze');

CREATE SCHEMA iceberg.silver
WITH (location = 'hdfs://namenode:9000/user/nifi/iceberg/silver');

CREATE SCHEMA iceberg.gold
WITH (location = 'hdfs://namenode:9000/user/nifi/iceberg/gold');


drop schema iceberg.bronze
drop schema iceberg.silver
drop schema iceberg.gold

drop table iceberg.bronze.info
drop table iceberg.bronze.visits 

CREATE TABLE iceberg.bronze.info (
    patient_id BIGINT COMMENT 'Mã định danh duy nhất của bệnh nhân',
    national_id VARCHAR(20) NOT NULL COMMENT 'Số định danh cá nhân / CCCD của bệnh nhân',
    full_name VARCHAR(255) NOT NULL COMMENT 'Họ và tên đầy đủ của bệnh nhân',
    age INT COMMENT 'Tuổi của bệnh nhân tại thời điểm cập nhật gần nhất',
    gender VARCHAR(10) COMMENT 'Giới tính của bệnh nhân (Nam/Nữ/Khác)',
    date_of_birth VARCHAR(20) COMMENT 'Ngày sinh (định dạng YYYY-MM-DD)',
    address VARCHAR(500) COMMENT 'Địa chỉ cư trú hiện tại của bệnh nhân',
    health_insurance_id VARCHAR(30) COMMENT 'Mã thẻ bảo hiểm y tế',
    phone VARCHAR(20) COMMENT 'Số điện thoại liên hệ của bệnh nhân',
    occupation VARCHAR(255) COMMENT 'Nghề nghiệp hiện tại của bệnh nhân',
    height_cm DECIMAL(5,2) COMMENT 'Chiều cao của bệnh nhân (cm)',
    weight_kg DECIMAL(5,2) COMMENT 'Cân nặng của bệnh nhân (kg)',
    bmi DECIMAL(4,1) COMMENT 'Chỉ số BMI = cân nặng / (chiều cao(m))^2',
    blood_type VARCHAR(5) COMMENT 'Nhóm máu (A, B, AB, O, Rh+/Rh-)',
    comorbidities VARCHAR(500) COMMENT 'Các bệnh nền hoặc tình trạng sức khỏe',
    created_at TIMESTAMP NOT NULL COMMENT 'Thời điểm bản ghi được tạo',
    updated_at TIMESTAMP NOT NULL COMMENT 'Thời điểm bản ghi được cập nhật gần nhất'
)
COMMENT 'Bảng lưu trữ hồ sơ y tế của bệnh nhân phục vụ phân tích và quản lý hồ sơ sức khỏe điện tử'
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(created_at)']
);


CREATE TABLE iceberg.bronze.visits (
    visit_id BIGINT COMMENT 'Mã định danh duy nhất cho mỗi lần khám',
    patient_id BIGINT NOT NULL COMMENT 'Tham chiếu đến patient_id trong bảng info',
    visit_datetime TIMESTAMP NOT NULL COMMENT 'Ngày và giờ diễn ra lần khám',
    doctor_id VARCHAR(20) NOT NULL COMMENT 'Mã định danh duy nhất của bác sĩ',
    doctor_name VARCHAR(255) COMMENT 'Họ và tên đầy đủ của bác sĩ',
    visit_type VARCHAR(20) COMMENT 'Loại lần khám: Định kỳ / Tự phát / Cấp cứu',
    department VARCHAR(255) COMMENT 'Khoa hoặc phòng ban bệnh viện',
    visit_reason VARCHAR(500) COMMENT 'Lý do khám hoặc triệu chứng chính',
    conclusion VARCHAR(1000) COMMENT 'Kết luận, khuyến nghị hoặc ghi chú theo dõi của bác sĩ',
    created_at TIMESTAMP NOT NULL COMMENT 'Thời điểm bản ghi được tạo',
    updated_at TIMESTAMP NOT NULL COMMENT 'Thời điểm bản ghi được cập nhật gần nhất'
)
COMMENT 'Bảng lưu trữ thông tin các lần khám của bệnh nhân phục vụ phân tích y tế và quản lý hồ sơ sức khỏe điện tử'
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(visit_datetime)']
);

select count(*) from iceberg.bronze.visits