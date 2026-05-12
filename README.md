# Student Management gRPC

Demo quản lý sinh viên bằng gRPC với Go. Chương trình minh họa đầy đủ 4 kiểu RPC phổ biến:

- Unary RPC: tạo sinh viên và lấy thông tin sinh viên.
- Server streaming RPC: server trả danh sách sinh viên theo stream.
- Client streaming RPC: client gửi nhiều điểm số, server tính điểm trung bình.
- Bidirectional streaming RPC: client và server trao đổi tin nhắn trong phiên thi.

## Cấu Trúc Thư Mục

```text
student_management_gRPC/
├── client/
│   └── main.go              # Chương trình client gọi các RPC
├── server/
│   └── main.go              # gRPC server và phần xử lý service
├── proto/
│   └── student.proto        # Định nghĩa message và service
├── pb/
│   ├── student.pb.go        # Code Go sinh từ protobuf
│   └── student_grpc.pb.go   # Code Go sinh cho gRPC service
├── go.mod
└── go.sum
```



## Các RPC Trong Service

Service được định nghĩa trong `proto/student.proto`:

```proto
service StudentService {
  rpc CreateStudent (CreateStudentRequest) returns (CreateStudentResponse);
  rpc GetStudent (GetStudentRequest) returns (CreateStudentResponse);
  rpc ListStudents (ListStudentsRequest) returns (stream CreateStudentResponse);
  rpc UploadScores (stream UploadScoresRequest) returns (UploadScoresResponse);
  rpc ExamSession (stream ExamMessage) returns (stream ExamMessage);
}
```

## Các Message Trong Proto

File `proto/student.proto` định nghĩa các `message` dùng làm kiểu dữ liệu khi client và server trao đổi với nhau.

| Message | Chức năng |
| --- | --- |
| `Student` | Mô tả thông tin cơ bản của sinh viên, gồm `id`, `name`, `age`, `class`. |
| `CreateStudentRequest` | Request client gửi lên để tạo sinh viên mới, gồm `name`, `email`, `age`, `class`. |
| `CreateStudentResponse` | Response server trả về sau khi tạo hoặc lấy thông tin sinh viên, gồm `id`, `name`, `age`, `class`. |
| `GetStudentRequest` | Request dùng để tìm sinh viên theo `id`. |
| `ListStudentsRequest` | Request dùng để lấy danh sách sinh viên, có trường `limit` để giới hạn số lượng kết quả. |
| `Score` | Mô tả điểm của một môn học, gồm `subject` và `score`. |
| `UploadScoresRequest` | Request được gửi nhiều lần trong client streaming, gồm `student_id` và một `Score`. |
| `UploadScoresResponse` | Response server trả về sau khi nhận xong danh sách điểm, gồm `student_id`, `total_subject`, `average_score`. |
| `ExamMessage` | Tin nhắn dùng trong bidirectional streaming, gồm `sender` và `message`. |

Ví dụ:

```proto
message CreateStudentRequest {
  string name  = 1;
  string email = 2;
  uint32 age   = 3;
  string class = 4;
}
```

Trong protobuf, các số như `= 1`, `= 2`, `= 3` là field number. Chúng được dùng để định danh field khi dữ liệu được serialize, không phải là giá trị mặc định.

## Service Trong Proto

`StudentService` là service chính của chương trình. Service này khai báo các hàm RPC mà client có thể gọi sang server.

| RPC | Kiểu RPC | Chức năng |
| --- | --- | --- |
| `CreateStudent` | Unary RPC | Client gửi một request tạo sinh viên, server trả về một response chứa sinh viên đã được tạo. |
| `GetStudent` | Unary RPC | Client gửi `id`, server trả về thông tin sinh viên tương ứng. |
| `ListStudents` | Server streaming RPC | Client gửi một request, server trả nhiều sinh viên lần lượt qua stream. |
| `UploadScores` | Client streaming RPC | Client gửi nhiều request điểm số, server tính tổng số môn và điểm trung bình rồi trả một response. |
| `ExamSession` | Bidirectional streaming RPC | Client và server cùng gửi, nhận nhiều tin nhắn trong cùng một phiên stream. |

Ý nghĩa các kiểu RPC:

- Unary RPC: một request, một response.
- Server streaming RPC: một request, nhiều response.
- Client streaming RPC: nhiều request, một response.
- Bidirectional streaming RPC: nhiều request và nhiều response, hai chiều có thể diễn ra song song.


## Sinh Lại Code Từ Proto

Nếu chỉnh sửa `proto/student.proto`, có thể sinh lại file trong thư mục `pb` bằng lệnh:

```powershell
protoc --proto_path=proto `
  --go_out=pb --go_opt=paths=source_relative `
  --go-grpc_out=pb --go-grpc_opt=paths=source_relative `
  student.proto
```
