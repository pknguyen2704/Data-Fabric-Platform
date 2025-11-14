-- Create table
create schema patients
CREATE TABLE patients.visits (
    visit_id        BIGINT PRIMARY KEY,
    patient_id      BIGINT NOT NULL,
    visit_datetime  TIMESTAMP NOT NULL,
    doctor_id       VARCHAR(20) NOT NULL,
    doctor_name     VARCHAR(255),
    visit_type      VARCHAR(20) CHECK (visit_type IN ('Định kỳ','Tự phát','Cấp cứu')),
    department      VARCHAR(255),
    visit_reason    VARCHAR(500),
    conclusion      TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Add comments for each column

COMMENT ON TABLE patients.visits IS 'Bảng lưu trữ thông tin các lần khám của bệnh nhân phục vụ phân tích y tế và quản lý hồ sơ sức khỏe điện tử';
COMMENT ON COLUMN patients.visits.visit_id IS 'Mã định danh duy nhất cho mỗi lần khám';
COMMENT ON COLUMN patients.visits.patient_id IS 'Tham chiếu đến Patient_ID trong bảng patient_info';
COMMENT ON COLUMN patients.visits.visit_datetime IS 'Ngày và giờ diễn ra lần khám';
COMMENT ON COLUMN patients.visits.doctor_id IS 'Mã định danh duy nhất của bác sĩ';
COMMENT ON COLUMN patients.visits.doctor_name IS 'Họ và tên đầy đủ của bác sĩ';
COMMENT ON COLUMN patients.visits.visit_type IS 'Loại lần khám: Định kỳ / Tự phát / Cấp cứu';
COMMENT ON COLUMN patients.visits.department IS 'Khoa hoặc phòng ban bệnh viện';
COMMENT ON COLUMN patients.visits.visit_reason IS 'Lý do khám hoặc triệu chứng chính';
COMMENT ON COLUMN patients.visits.conclusion IS 'Kết luận, khuyến nghị hoặc ghi chú theo dõi của bác sĩ';
COMMENT ON COLUMN patients.visits.created_at IS 'Thời điểm bản ghi được tạo';
COMMENT ON COLUMN patients.visits.updated_at IS 'Thời điểm bản ghi được cập nhật gần nhất';

-- Create function to auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = CURRENT_TIMESTAMP;
   RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
CREATE TRIGGER trigger_update_patient_visits
BEFORE UPDATE ON patients.visits
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();


CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

SELECT * FROM pg_stat_statements LIMIT 5;


select count(*) from patients.visits