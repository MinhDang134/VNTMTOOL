Dự án vntmtool

I. GIỚI THIỆU 

Trong bối cảnh thị trường cạnh tranh ngày càng khốc liệt, việc nắm bắt thông tin về các nhãn hiệu đã đăng ký hoặc đang trong quá trình xét duyệt đóng vai trò then chốt đối với doanh nghiệp và các chuyên gia sở hữu trí tuệ. Để đáp ứng nhu cầu này, chúng tôi xin giới thiệu một công cụ mạnh mẽ được thiết kế chuyên biệt để thu thập (crawl) toàn bộ dữ liệu nhãn hàng từ hệ thống Vietnam Trademark một cách tự động và hiệu quả.

II. CÁC TÍNH NĂNG CHÍNH 


+ crawl dữ liệu từ trang vietnamtrademark ( crawl đa luồng )
+ khi crawl về database sẽ chia dữ liệu với partition theo tháng 
+ tạo thêm bảng category chứa thông tin của 45 nhóm hàng và phân chia partition theo nhóm
+ tự động update những nhãn có trạng thái "Đang giải quyết" mỗi ngày vào lúc 0h
+ khi chương trình bị lỗi chỉ cần bật lại là sẽ tiếp tục thực hiện crawl từ nó 
+ thực hiện proxy xoay
+ giới hạn request khi crawl không quá 20 request mỗi phút cho 1 tiến trình  
+ tạo những quãng nghỉ giữa các lần crawl
+ hiển thị các log giúp người dùng biết được quá trình chạy 
+ chuyển đổi link ảnh của vietnamtrademark qua định dạng jpg và chứa nó vào folder trong dự án và lưu vào local và sau đó lưu vào database

III. CHUẨN BỊ MÔI TRƯỜNG 

CÔNG CỤ VÀ CÔNG NGHỆ SỬ DỤNG: Pycharm, pgadmin4, fastapi

THƯ VIỆN VÀ MÔI TRƯỜNG CẦN THIẾT :

requests==2.31.0

beautifulsoup4==4.12.2

selenium==4.15.2

webdriver-manager==4.0.1

sqlmodel

sqlalchemy==2.0.23

alembic==1.12.1

psycopg2-binary==2.9.9


python-dotenv==1.0.0

aiohttp==3.9.1

aiohttp-socks==0.8.4


schedule==1.2.1

APScheduler==3.10.4


python-json-logger==2.0.7

prometheus-client==0.19.0


pandas==2.1.3

numpy==1.26.2

tqdm==4.66.1

