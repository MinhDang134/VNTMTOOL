vntmtool_iCheck

link_github_duan : https://github.com/MinhDang134/VNTMTOOL
> dịch vụ dành cho doanh nghiệp/cá nhân cần dữ liệu nhãn hàng từ trang : https://vietnamtrademark.net/

## MỤC LỤC 

- [Tổng quan dự án](#tổng-quan-dự-án)
- [Các tính năng chính](#các-tính-năng-chính)
- [Công nghệ sử dụng](#công-nghệ-sử-dụng)
- [Yêu cầu hệ thống](#yêu-cầu-hệ-thống)
- [Hướng dẫn cài đặt](#hướng-dẫn-cài-đặt)
- [Cấu hình](#cấu-hình)
- [Hướng dẫn sử dụng](#hướng-dẫn-sử-dụng)
- [Xử lý sự cố thường gặp](#xử-lý-sự-cố-thường-gặp)
- [Thông tin liên hệ & Hỗ trợ](#thông-tin-liên-hệ--hỗ-trợ)

## Tổng quan dự án
* Dự án giải quyết vấn đề gì?
  - Tự động hóa lấy thông tin nhãn hàng từ trang vietnamtrademark, giúp đội ngũ chăm sóc khách hàng, dịch vụ pháp lý... tra cứu tránh tình trạng bị trang vietnamtrademark coi là mối
    đe dọa.
* Đối tương người dùng là ai?
  - Nhân viên bộ phận dịch vụ trực tiếp làm việc với khách hàng về vấn đề nhãn hàng,đăng ký kinh doanh... .
* Lợi ích chính của dự án mang lại là?
  - Giúp tránh tình trạng bị chặn ip cũng như có thể sử dụng dữ liệu một cách tự do, tăng tốc độ làm việc với khách hàng.
  
## Các tính năng chính 
* Crawl
 - Crawl dữ liệu theo ngày tùy ý, tự động chuyển trang khi đã xử lý xong một trang dữ liệu cho đến khi không còn dữ liệu.
 - Crawl dữ liệu theo nhiều luồng song song giúp tăng tốc độ crawl dữ liệu.
 - Tự động lưu logs về lỗi,quá trình chạy, chi tiết luồng chạy, ngày hoàn thành gần nhất.
 - Xử lý khi gặp lỗi : bỏ qua nhãn bị lỗi, in vào logs và tiếp tục chạy.
 - Giới hạn số lần gửi request tránh bị coi là mối đe dọa.
 - Sử dụng nhiều proxy luân phiên nhau tránh lộ ip thật, cũng như tránh bị coi là mối đe dọa.
 - Thực hiện quy trình chạy 10 Phút và nghỉ 5 Phút tránh bị coi là mối đe dọa.
* Database
 - Tạo Bảng với tên là "Brand" duy nhất để lưu dữ liệu.
 - Chia partition theo tháng để chia ra nhiều phần nhỏ của bảng Brand giúp truy xuất và quản lý dự liệu nhanh hơn và tường minh hơn.
 - Tạo Bảng với tên là "Category" kế thừa dữ liệu từ bảng "Brand".
 - Chia partition theo nhóm của nhãn hàng, giúp dễ dàng tìm kiếm nhãn hàng theo nhóm nhanh hơn cũng như tường minh hơn.
* Update 
 - Tự Dộng Update những dữ liệu có trạng thái "Đang giải quyết" trong database mỗi ngày (" với điều kiện được tìm kiếm >=5 lần và thời gian chưa được cập nhật 1 tháng ").
* Telegram
 - Tự Động gửi thông báo đến telegram_bot khi bắt đầu, gặp lỗi, kết thúc.
* Docker 
 - đóng gói toàn bộ dữ liệu vào một docker để dễ dàng gửi cho khách hàng 

## Công nghệ sử dụng 
**Backend:** fastapi, sqlaichemy, sqlmodel..

**Database:** Postgresql

**Deployment:** Docker 

## Yêu Cầu hệ thống
Hệ điều hành  :	Windows 10 (64-bit) hoặc macOS 10.14 Windows 11 hoặc macOS 12 trở lên

Bộ nhớ (RAM)  : 12 GB RAM	

Bộ xử lý (CPU): Intel Core i3 (thế hệ 8+) / AMD Ryzen 3 hoặc Intel Core i5 (thế hệ 8+) / AMD Ryzen 5

Ổ cứng	      : Ổ cứng SSD với 20 GB trống hoặc Ổ cứng SSD NVMe với 20 GB trống

## Hướng dẫn cài đặt 
1. Clone a repository 
   - Tạo một folder chứa dự án bằng terminal : " mkdir ten_du_an " 
   - Điền " cd ten_du_an "
   - Nhập vào terminal "git clone  https://github.com/MinhDang134/VNTMTOOL.git"
   - Mở dự án bằng Pycharm 
   
2. Cài đặt môi trường 
   - Sau khi đã mở dự án trong Pycharm ta vào terminal tạo môi trường như sau, lần lượt là " python -m venv venv " và " source .venv/bin/activate " để kích hoạt môi trường
   - Tiếp theo vào vào phần 4 gạch góc trái màn hình làm lần lượt như sau " setting -> Project: ten_du_an -> Python interpreter -> Add interpreter -> Add local interpreter
     -> chọn môi trường -> Apply " 
   - Tiếp đó tải những thư viện sau từ file requirements/requirements.txt

					requests==2.31.0
					beautifulsoup4==4.12.2
					selenium==4.15.2
					webdriver-manager==4.0.1
					sqlalchemy==2.0.23
					alembic==1.12.1
					psycopg2-binary==2.9.9
					python-dotenv==1.0.0
					aiohttp==3.9.1
					aiohttp-socks==0.8.4
					sqlmodel
					pydantic-settings
					httpx
					schedule==1.2.1
					APScheduler==3.10.4
					python-json-logger==2.0.7
					prometheus-client==0.19.0
					pandas==2.1.3
					numpy==1.26.2
					tqdm==4.66.1
					
   - Tiếp theo đó ta tạo một database có tên là "vntmtool" và thực hiện tạo các bảng là "Brand" và "Categogy"
   - Tiếp theo đó khởi tạo bảng "Brand" với chức năng chia partiton với nội dung là :
   
					CREATE TABLE IF NOT EXISTS public.brand
					(
					    id integer NOT NULL DEFAULT nextval('brand_id_seq'::regclass),
					    brand_name text COLLATE pg_catalog."default",
					    image_url text COLLATE pg_catalog."default",
					    product_group text COLLATE pg_catalog."default",
					    status text COLLATE pg_catalog."default",
					    application_date date NOT NULL,
					    application_number text COLLATE pg_catalog."default" NOT NULL,
					    applicant text COLLATE pg_catalog."default",
					    representative text COLLATE pg_catalog."default",
					    product_detail text COLLATE pg_catalog."default",
					    va_count integer NOT NULL DEFAULT 0,
					    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
					    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
					    CONSTRAINT brand_pkey PRIMARY KEY (id, application_number, application_date)
					) PARTITION BY RANGE (application_date);
					
   - Tiếp theo đó tạo thêm bảng "Category" với chức năng chia partiton và insert dữ liệu từ bảng Brand nội dung là :
   
                                        CREATE TABLE IF NOT EXISTS public.category
					(
					    nice_partition integer NOT NULL, 
					    id integer NOT NULL,            
					    category_name text COLLATE pg_catalog."default", 
					    image_url text COLLATE pg_catalog."default",    
					    product_group text COLLATE pg_catalog."default", 
					    status text COLLATE pg_catalog."default",        
					    application_date date NOT NULL,                  
					    application_number text COLLATE pg_catalog."default",
					    applicant text COLLATE pg_catalog."default",   
					    representative text COLLATE pg_catalog."default",
					    product_detail text COLLATE pg_catalog."default",
					    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
					    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
					    CONSTRAINT category_pkey PRIMARY KEY (id, application_date, nice_partition)
					) PARTITION BY LIST (nice_partition);

					select * from category
					drop table category

					DO $$
					DECLARE
					    i integer;
					BEGIN
					    FOR i IN 1..45 LOOP
						IF NOT EXISTS (
						    SELECT 1
						    FROM   pg_class c
						    JOIN   pg_namespace n ON n.oid = c.relnamespace
						    WHERE  c.relname = 'category_p' || i  
						    AND    n.nspname = 'public'
						) THEN
						    EXECUTE format(
							'CREATE TABLE public.category_p%s PARTITION OF public.category FOR VALUES IN (%s);',
							i, i
						    );
						    RAISE NOTICE 'Đã tạo partition public.category_p%', i;
						ELSE
						    RAISE NOTICE 'Partition public.category_p% đã tồn tại.', i;
						END IF;
					    END LOOP;
					END $$;


					INSERT INTO public.category (
					    id,
					    nice_partition,
					    category_name,
					    image_url,
					    product_group,
					    status,
					    application_date,
					    application_number,
					    applicant,
					    representative,
					    product_detail
					)
					SELECT
					    b.id,
					    CAST(TRIM(pg_data.group_value) AS INTEGER) AS nice_partition,
					    b.brand_name AS category_name,
					    b.image_url,
					    b.product_group, 
					    b.status,
					    b.application_date,
					    b.application_number,
					    b.applicant,
					    b.representative,
					    b.product_detail
					FROM
					    public.brand b
					JOIN LATERAL 
					    unnest(string_to_array(b.product_group, ',')) AS pg_data(group_value) ON true -- Unnest và đặt alias
					WHERE
					    b.id IS NOT NULL
					    
					    AND TRIM(pg_data.group_value) ~ '^[0-9]+$'
					    
					    AND CAST(TRIM(pg_data.group_value) AS INTEGER) BETWEEN 1 AND 45
					ON CONFLICT (id, application_date, nice_partition) DO NOTHING;

					DO $$
					BEGIN
					    RAISE NOTICE 'Quá trình tạo bảng category, các partition và chuyển dữ liệu (với ID từ bảng brand) đã hoàn tất.';
					    RAISE NOTICE 'Kiểm tra số lượng bản ghi trong bảng category: %', (SELECT count(*) FROM public.category);
					END $$;
					
					
  - Sau khi đã tạo ra các bảng thì ta liên kế với Pycharm bằng cách chọn postgresql từ phần có icon hình database nhập username,password và tên database rồi ấn apply.
  - sau khi làm tất cả các bước trên ta một chạy dữ án ta thực hiện mở terminal và gõ " Python run_scraper.py " khi đó quá trình crawl sẽ được thực thi 
 

## Cấu hình 
 - Tạo ra một file .env 
 - điền nội dung file như sau :
				DATABASE_URL = postgresql://user:pass@localhost:port/database_name
				DB_USER = user
				DB_PASSWORD = pass
				DB_NAME = database_name

				IP_ONE = ...
				IP_TWO = ...
				IP_THREE = ...


				USER_NAME = ...
				USER_PASSWORD = ...

				PORT_ONE = ...
				PORT_TWO = ...
				PORT_THREE = ...

				REQUEST_LIMIT= ...
				REQUEST_DELAY= ...

				INITIAL_SCRAPE_START_YEAR= ...
				INITIAL_SCRAPE_START_MOTH= ...
				INITIAL_SCRAPE_START_DAY= ...


				OVERALL_SCRAPE_END_YEAR= ...
				OVERALL_SCRAPE_END_MOTH= ...
				OVERALL_SCRAPE_END_DAY= ...



				RUN_DURATION_MINUTES= ...
				PAUSE_DURATION_MINUTES= ...

				MIN_DELAY_CHECK_PENDING= ... 
				MAX_DELAY_CHECK_PENDING= ...


				LOCAL_MEDIA_BASE_URL = ...
				SOURCE_WEBSITE_DOMAIN = https://vietnamtrademark.net
				MAX_REQUEST_RETRIES = ...
				REQUEST_TIMEOUT = ...
				SSL_VERIFY_REQUEST = ...
				SSL_VERIFY_DOWNLOAD = ...
				DOWNLOAD_TIMEOUT = ...
				REQUEST_LIMIT_PER_INTERVAL = ...
				REQUEST_INTERVAL_SECONDS = ...
				MIN_REQUEST_DELAY = ...
				MAX_REQUEST_DELAY = ...
				MEDIA_BRAND_IMAGES_SUBPATH = ...

				CONCURRENT_SCRAPING_TASKS= ...

				#proxy tele_bot

				BOT_TOKEN = ...
				CHAT_ID = ...


				PROXY_LOGIN = user
				PROXY_PASSWORD = pass
				PROXY_IP_HTTP = ip
				PROXY_PORT_HTTP = port
				PROXY_URL = socks5://user:pass@ip:port
## Hướng dẫn sử dụng 
** Chạy giao diện dashboard xem quá trình chạy **
   - Mở terminal và gõ "uvicorn src.dashboard.dashboard:app --reload --port 5001"

** Chạy chức năng crawl dữ liệu **
   - Mở terminal và gõ "python run_scraper.py"

** Chạy docker-compose**
   - Mở terminal và gõ "docker-compose up --build"
   - Những câu lệnh cơ bản : 
   - 
                             "docker ps" : xem các docker container đang hoạt động
   - 
                             "docker-compose ps" : những service đang hoạt động
   - 
                             "docker images" : những docker images đang hoạt động
                             ......
                             
## Xử lý sự cố thường gặp
* **Lỗi "Cannot connect to Database"**:
    * Kiểm tra lại thông tin `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASS` trong file `.env`.
    * Đảm bảo dịch vụ MongoDB đang chạy trên máy của bạn.
    

## Thông tin liên hệ & Hỗ trợ
**intern backend python :** Nguyễn Minh Đăng (" ttwlmobile@gmail.com ")

					   
