package main

import (
    "context"
    "errors"
    "flag"
    "fmt"
    "log"
    "net"
    "runtime"
    "sync"
    "sync/atomic"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"
)

const (
    messageSizeBytes = 1000 * 1024
)

var (
    host        = flag.String("host", "localhost", "RabbitMQ host")
    port        = flag.Int("port", 5672, "RabbitMQ port")
    username    = flag.String("username", "guest", "RabbitMQ username")
    password    = flag.String("password", "guest", "RabbitMQ password")
    vhost       = flag.String("vhost", "/", "RabbitMQ virtual host")
    queueName   = flag.String("queue", "benchmark", "Target queue name")
    queueParts  = flag.Int("queue-partitions", 1, "Number of random queue partitions (suffix 0..n-1)")
    messages    = flag.Int64("messages", 0, "Messages per worker (0 means run indefinitely)")
    persistent  = flag.Bool("persistent", false, "Use delivery mode persistent for messages")
    connTimeout = flag.Duration("connection-timeout", 5*time.Second, "Dial timeout per message")
    pubTimeout  = flag.Duration("publish-timeout", 5*time.Second, "Publish timeout per message")
)

func main() {
    flag.Parse()

    concurrency := runtime.GOMAXPROCS(0)
    if concurrency <= 0 {
        log.Fatalf("invalid GOMAXPROCS: %d", concurrency)
    }

    if *messages < 0 {
        log.Fatalf("messages must be non-negative: %d", *messages)
    }

    body := make([]byte, messageSizeBytes)
    for i := range body {
        body[i] = byte('A' + i%26)
    }

    url := fmt.Sprintf("amqp://%s:%s@%s:%d/%s", *username, *password, *host, *port, *vhost)

    log.Printf("starting producer: concurrency=%d queue=%s host=%s port=%d vhost=%s user=%s",
        concurrency, *queueName, *host, *port, *vhost, *username)

    var wg sync.WaitGroup
    var totalSent atomic.Int64
    var totalErrors atomic.Int64

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Ensure queues exist once at startup (separate short-lived connection)
    {
        dialer := amqp.DefaultDial(*connTimeout)
        cfg := amqp.Config{Dial: dialer, Heartbeat: 0, Locale: "en_US"}
        conn, err := amqp.DialConfig(url, cfg)
        if err != nil {
            log.Fatalf("ensure dial: %v", err)
        }
        if err := declareQueues(conn, *queueName, *queueParts); err != nil {
            log.Fatalf("ensure declare: %v", err)
        }
        conn.Close()
    }

    // periodic stats
    statsTicker := time.NewTicker(10 * time.Second)
    defer statsTicker.Stop()
    go func() {
        for {
            select {
            case <-ctx.Done():
                return
            case <-statsTicker.C:
                log.Printf("stats: sent=%d errors=%d goroutines=%d",
                    totalSent.Load(), totalErrors.Load(), runtime.NumGoroutine())
            }
        }
    }()

    // Multiple shared connections across producer goroutines (one per GOMAXPROCS)
    sharedConns := make([]*amqp.Connection, concurrency)
    connMus := make([]sync.RWMutex, concurrency)

    reconnect := func(idx int) error {
        dialer := amqp.DefaultDial(*connTimeout)
        cfg := amqp.Config{Dial: dialer, Heartbeat: 0, Locale: "en_US"}
        c, err := amqp.DialConfig(url, cfg)
        if err != nil {
            return err
        }
        connMus[idx].Lock()
        if sharedConns[idx] != nil {
            _ = sharedConns[idx].Close()
        }
        sharedConns[idx] = c
        connMus[idx].Unlock()
        return nil
    }

    for i := 0; i < concurrency; i++ {
        if err := reconnect(i); err != nil {
            log.Fatalf("initial dial failed for connection %d: %v", i, err)
        }
    }

    // One producer goroutine per (partition × connection); groups share their connection, own channels.
    // Total concurrent producers == queuePartitions × GOMAXPROCS (or GOMAXPROCS if partitions <= 1).
    var totalProducers int
    if *queueParts <= 1 {
        totalProducers = 1
    } else {
        totalProducers = *queueParts
    }

    // Spawn producers per connection index and partition. Each goroutine manages its own connection.
    for connIdx := 0; connIdx < concurrency; connIdx++ {
        wg.Add(totalProducers)
        for part := 0; part < totalProducers; part++ {
            go func(connIndex int, partition int) {
                defer wg.Done()

                qn := *queueName
                if *queueParts > 1 {
                    qn = fmt.Sprintf("%s_%d", *queueName, partition)
                }

                var produced int64
                var ch *amqp.Channel

                for {
                    if *messages > 0 && produced >= *messages {
                        return
                    }

                    if ch == nil {
                        connMus[connIndex].RLock()
                        c := sharedConns[connIndex]
                        connMus[connIndex].RUnlock()
                        if c == nil {
                            if err := reconnect(connIndex); err != nil {
                                totalErrors.Add(1)
                                log.Printf("conn %d part %d: reconnect error: %v", connIndex, partition, err)
                                time.Sleep(100 * time.Millisecond)
                                continue
                            }
                            continue
                        }
                        var err error
                        ch, err = c.Channel()
                        if err != nil {
                            totalErrors.Add(1)
                            log.Printf("conn %d part %d: channel error: %v", connIndex, partition, err)
                            _ = c.Close()
                            if err2 := reconnect(connIndex); err2 != nil {
                                totalErrors.Add(1)
                                log.Printf("conn %d part %d: reconnect error: %v", connIndex, partition, err2)
                            }
                            ch = nil
                            time.Sleep(50 * time.Millisecond)
                            continue
                        }
                    }

                    publishCtx, cancel := context.WithTimeout(ctx, *pubTimeout)
                    deliveryMode := amqp.Transient
                    if *persistent {
                        deliveryMode = amqp.Persistent
                    }
                    err := ch.PublishWithContext(
                        publishCtx,
                        "",
                        qn,
                        false,
                        false,
                        amqp.Publishing{
                            ContentType:  "application/octet-stream",
                            DeliveryMode: deliveryMode,
                            Body:         body,
                            Timestamp:    time.Now().UTC(),
                        },
                    )
                    cancel()
                    if err != nil {
                        totalErrors.Add(1)
                        log.Printf("conn %d part %d: publish error queue=%s class=%s err=%v", connIndex, partition, qn, classifyError(err), err)
                        _ = ch.Close()
                        ch = nil
                        time.Sleep(50 * time.Millisecond)
                        continue
                    }

                    sent := totalSent.Add(1)
                    produced++
                    if produced%1000 == 0 {
                        log.Printf("conn %d part %d: produced %d messages to %s (total=%d)", connIndex, partition, produced, qn, sent)
                    }
                }
            }(connIdx, part)
        }
    }

    wg.Wait()

    for i := 0; i < concurrency; i++ {
        connMus[i].RLock()
        if sharedConns[i] != nil {
            _ = sharedConns[i].Close()
        }
        connMus[i].RUnlock()
    }
}

func publishOnceURL(parent context.Context, url string, queue string, body []byte, persistent bool, connTimeout time.Duration, pubTimeout time.Duration) error {
    dialer := amqp.DefaultDial(connTimeout)
    cfg := amqp.Config{Dial: dialer, Heartbeat: 0, Locale: "en_US"}
    conn, err := amqp.DialConfig(url, cfg)
    if err != nil {
        return fmt.Errorf("dial: %w", err)
    }
    // Close the connection quickly without waiting for broker acks on Close
    // We still defer Close to ensure sockets are returned to OS promptly
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        return fmt.Errorf("channel: %w", err)
    }
    defer ch.Close()

    if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
        return fmt.Errorf("queue declare: %w", err)
    }

    deliveryMode := amqp.Transient
    if persistent {
        deliveryMode = amqp.Persistent
    }

    publishCtx, cancel := context.WithTimeout(parent, pubTimeout)
    defer cancel()

    if err := ch.PublishWithContext(
        publishCtx,
        "",
        queue,
        false,
        false,
        amqp.Publishing{
            ContentType: "application/octet-stream",
            DeliveryMode: deliveryMode,
            Body:         body,
            Timestamp:    time.Now().UTC(),
        },
    ); err != nil {
        return fmt.Errorf("publish: %w", err)
    }

    return nil
}

func declareQueues(conn *amqp.Connection, base string, parts int) error {
    ch, err := conn.Channel()
    if err != nil {
        return fmt.Errorf("channel: %w", err)
    }
    defer ch.Close()

    if parts <= 1 {
        if _, err := ch.QueueDeclare(base, true, false, false, false, nil); err != nil {
            return err
        }
        return nil
    }
    for i := 0; i < parts; i++ {
        name := fmt.Sprintf("%s_%d", base, i)
        if _, err := ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
            return err
        }
    }
    return nil
}

func classifyError(err error) string {
    if err == nil {
        return "none"
    }
    var netErr net.Error
    if errors.As(err, &netErr) {
        if netErr.Timeout() {
            return "net-timeout"
        }
        return "net"
    }
    var amqErr *amqp.Error
    if errors.As(err, &amqErr) {
        return fmt.Sprintf("amqp-%d", amqErr.Code)
    }
    return "other"
}

