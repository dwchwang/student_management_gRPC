package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"student_management_gRPC/pb"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type studentServer struct {
	pb.UnimplementedStudentServiceServer
	students map[string]*pb.CreateStudentResponse
	scores   map[string][]*pb.Score
}

// unary create student
func (s *studentServer) CreateStudent(
	ctx context.Context,
	req *pb.CreateStudentRequest,
) (*pb.CreateStudentResponse, error) {
	// validate name va class
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Name khong duoc de trong")
	}

	if req.Email == "" {
		return nil, status.Error(codes.InvalidArgument, "Email khong duoc de trong")
	}
	id := fmt.Sprintf("user-%d", len(s.students)+1)
	student := &pb.CreateStudentResponse{
		Id:    id,
		Name:  req.Name,
		Class: req.Class,
		Age:   req.Age,
	}

	s.students[id] = student

	log.Printf("Đã tạo student: %s - %s", id, req.Name)
	return student, nil
}

// unary get student
func (s *studentServer) GetStudent(
	ctx context.Context,
	req *pb.GetStudentRequest,
) (*pb.CreateStudentResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "id không được để trống")
	}
	student, exists := s.students[req.Id]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "không tìm thấy student với id: %s", req.Id)
	}
	return student, nil
}

// server streaming List students
func (s *studentServer) ListStudents(
	req *pb.ListStudentsRequest,
	stream pb.StudentService_ListStudentsServer,
) error {
	count := int32(0)
	for _, student := range s.students {
		if count >= req.Limit {
			break
		}

		// gui tung user qua 1 stream
		if err := stream.Send(student); err != nil {
			return status.Errorf(codes.Internal, "lỗi gửi stream: %v", err)
		}
		log.Printf("Stream student: %s", student.Name)
		count++
		// Gia lap delay giua cac message
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}

// client streaming
func (s *studentServer) UploadScores(
	stream pb.StudentService_UploadScoresServer,
) error {
	var (
		studentID     string
		totalSubjects int32
		totalScore    float32
	)
	// lap nhan tung req du lieu tu client
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "lỗi nhận stream: %v", err)
		}
		// tich luy data cua tung chunk
		studentID = req.StudentId
		totalScore += req.Score.Score
		totalSubjects++
		log.Printf("Nhận điểm: %s - %s: %.1f",
			req.StudentId, req.Score.Subject, req.Score.Score)
	}

	if _, exists := s.students[studentID]; !exists {
		return status.Errorf(codes.NotFound,
			"không tìm thấy student với id: %s", studentID)
	}
	average := float32(0)
	if totalSubjects > 0 {
		average = totalScore / float32(totalSubjects)
	}
	return stream.SendAndClose(&pb.UploadScoresResponse{
		StudentId:    studentID,
		TotalSubject: totalSubjects,
		AverageScore: average,
	})
}

// bidi streaming
func (s *studentServer) ExamSession(
	stream pb.StudentService_ExamSessionServer,
) error {

	log.Println("ExamSession bắt đầu")
	for {
		// nhan message tu client
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Println("ExamSession kết thúc")
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "lỗi nhận message: %v", err)
		}
		log.Printf("[%s]: %s", msg.Sender, msg.Message)
		// Echo lại kèm prefix
		reply := &pb.ExamMessage{
			Sender:  "server",
			Message: fmt.Sprintf("[Server]: %s", msg.Message),
		}
		if err := stream.Send(reply); err != nil {
			return status.Errorf(codes.Internal, "lỗi gửi reply: %v", err)
		}
	}
}

// logging interceptor
func loggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()
	log.Printf("→ [%s] bắt đầu", info.FullMethod)
	resp, err := handler(ctx, req)
	duration := time.Since(start)
	if err != nil {
		st, _ := status.FromError(err)
		log.Printf("← [%s] lỗi | %s | %v", info.FullMethod, st.Code(), duration)
	} else {
		log.Printf("← [%s] thành công | %v", info.FullMethod, duration)
	}

	return resp, err
}

// main
func main() {
	// mo TCP port 
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal("Khong the listen port: %w", err)
	}

	// Tao gRPC server
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			loggingInterceptor,
		),
	)

	// Dang ky service student vao server
	pb.RegisterStudentServiceServer(grpcServer, &studentServer{
		students: make(map[string]*pb.CreateStudentResponse),
		scores: make(map[string][]*pb.Score),
	})
	
	log.Println("gRPC server đang chạy tại :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Lỗi serve: %v", err)
	}
}
