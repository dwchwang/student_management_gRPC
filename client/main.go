package main

import (
	"context"
	"io"
	"log"
	"student_management_gRPC/pb"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func main() {
	// Tao connection den server
	conn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal("Khong the ket noi: %w", err)
	}
	defer conn.Close()

	// Tao client stub
	client := pb.NewStudentServiceClient(conn)

	// Metadata dùng chung cho tất cả requests
	md := metadata.Pairs("authorization", "Bearer student-token")

	// ── 1. Unary: CreateStudent x3 ──────────────────
	log.Println("=== 1. CreateStudent ===")

	studentNames := []struct {
		name  string
		email string
		age   uint32
		class string
	}{
		{"Nguyễn Văn An", "an@gmail.com", 18, "12A1"},
		{"Trần Thị Bình", "binh@gmail.com", 17, "11B2"},
		{"Lê Hoàng Nam", "nam@gmail.com", 19, "12A2"},
	}

	var studentIDs []string

	for _, s := range studentNames {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		ctx = metadata.NewOutgoingContext(ctx, md)

		resp, err := client.CreateStudent(ctx, &pb.CreateStudentRequest{
			Name:  s.name,
			Age:   s.age,
			Email: s.email,
			Class: s.class,
		})
		cancel()

		if err != nil {
			st, _ := status.FromError(err)
			log.Printf("CreateStudent lỗi [%s]: %s", st.Code(), st.Message())
			continue
		}

		log.Printf("Đã tạo: ID=%s | Name=%s | Class=%s",
			resp.Id, resp.Name, resp.Class)
		studentIDs = append(studentIDs, resp.Id)
	}

	// ── 2. Unary: GetStudent ─────────────────────────
	log.Println("\n=== 2. GetStudent ===")

	// Gọi với id hợp lệ
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ctx = metadata.NewOutgoingContext(ctx, md)

	resp, err := client.GetStudent(ctx, &pb.GetStudentRequest{
		Id: studentIDs[0],
	})
	cancel()

	if err != nil {
		st, _ := status.FromError(err)
		log.Printf("GetStudent lỗi [%s]: %s", st.Code(), st.Message())
	} else {
		log.Printf("Tìm thấy: ID=%s | Name=%s | Class=%s",
			resp.Id, resp.Name, resp.Class)
	}

	// Gọi với id không tồn tại
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	ctx = metadata.NewOutgoingContext(ctx, md)

	_, err = client.GetStudent(ctx, &pb.GetStudentRequest{
		Id: "invalid-id",
	})
	cancel()

	if err != nil {
		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.NotFound:
			log.Printf("Student không tồn tại: %s", st.Message())
		case codes.InvalidArgument:
			log.Printf("Sai input: %s", st.Message())
		default:
			log.Printf("Lỗi khác [%s]: %s", st.Code(), st.Message())
		}
	}

	// ── 3. Server Streaming: ListStudents ───────────
	log.Println("\n=== 3. ListStudents ===")

	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	ctx = metadata.NewOutgoingContext(ctx, md)
	defer cancel()

	listStream, err := client.ListStudents(ctx, &pb.ListStudentsRequest{
		Limit: 10,
	})
	if err != nil {
		log.Fatalf("ListStudents lỗi: %v", err)
	}

	for {
		student, err := listStream.Recv()
		if err == io.EOF {
			log.Println("ListStudents stream kết thúc")
			break
		}
		if err != nil {
			st, _ := status.FromError(err)
			log.Printf("Lỗi nhận stream [%s]: %s", st.Code(), st.Message())
			break
		}
		log.Printf("Nhận student: ID=%s | Name=%s | Class=%s",
			student.Id, student.Name, student.Class)
	}

	// ── 4. Client Streaming: UploadScores ───────────
	log.Println("\n=== 4. UploadScores ===")

	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	ctx = metadata.NewOutgoingContext(ctx, md)
	defer cancel()

	uploadStream, err := client.UploadScores(ctx)
	if err != nil {
		log.Fatalf("UploadScores lỗi: %v", err)
	}

	scores := []struct {
		subject string
		score   float32
	}{
		{"Toán", 8.5},
		{"Văn", 7.0},
		{"Anh", 9.0},
	}

	for _, s := range scores {
		err := uploadStream.Send(&pb.UploadScoresRequest{
			StudentId: studentIDs[0],
			Score: &pb.Score{
				Subject: s.subject,
				Score:   s.score,
			},
		})
		if err != nil {
			log.Fatalf("Lỗi gửi score: %v", err)
		}
		log.Printf("Đã gửi: %s - %.1f", s.subject, s.score)
		time.Sleep(300 * time.Millisecond)
	}

	uploadResp, err := uploadStream.CloseAndRecv()
	if err != nil {
		st, _ := status.FromError(err)
		log.Printf("UploadScores lỗi [%s]: %s", st.Code(), st.Message())
	} else {
		log.Printf("Kết quả: StudentID=%s | Tong so Mon=%d | Trung bình=%.2f",
			uploadResp.StudentId,
			uploadResp.TotalSubject,
			uploadResp.AverageScore,
		)
	}

	// ── 5. Bidi Streaming: ExamSession ──────────────
	log.Println("\n=== 5. ExamSession ===")

	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	ctx = metadata.NewOutgoingContext(ctx, md)
	defer cancel()

	examStream, err := client.ExamSession(ctx)
	if err != nil {
		log.Fatalf("ExamSession lỗi: %v", err)
	}

	// Goroutine RECV: nhận reply từ server
	waitReceive := make(chan struct{})
	go func() {
		defer close(waitReceive)
		for {
			msg, err := examStream.Recv()
			if err == io.EOF {
				log.Println("ExamSession stream kết thúc")
				return
			}
			if err != nil {
				st, _ := status.FromError(err)
				log.Printf("Lỗi nhận ExamSession [%s]: %s", st.Code(), st.Message())
				return
			}
			log.Printf("Nhận từ server: [%s]: %s", msg.Sender, msg.Message)
		}
	}()

	// Goroutine SEND (main): gửi messages lên server
	messages := []string{
		"Câu 1: 1 + 1 = ?",
		"Câu 2: Thủ đô Việt Nam là gì?",
		"Câu 3: gRPC là viết tắt của gì?",
	}

	for _, text := range messages {
		err := examStream.Send(&pb.ExamMessage{
			Sender:  "client",
			Message: text,
		})
		if err != nil {
			log.Fatalf("Lỗi gửi ExamSession: %v", err)
		}
		log.Printf("Đã gửi: %s", text)
		time.Sleep(1 * time.Second)
	}

	// Báo server client gửi xong
	examStream.CloseSend()

	// Chờ goroutine RECV xử lý xong
	<-waitReceive
	log.Println("\n Hoan thanh chuong trinh")
}
