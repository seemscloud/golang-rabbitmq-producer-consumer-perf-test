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

var (
  host        = flag.String("host", "localhost", "RabbitMQ host")
  port        = flag.Int("port", 5672, "RabbitMQ port")
  username    = flag.String("username", "guest", "RabbitMQ username")
  password    = flag.String("password", "guest", "RabbitMQ password")
  vhost       = flag.String("vhost", "/", "RabbitMQ virtual host")
  queueName   = flag.String("queue", "benchmark", "Base queue name to consume from")
  queueParts  = flag.Int("queue-partitions", 1, "Number of queue partitions (suffix 0..n-1)")
  messages    = flag.Int64("messages", 0, "Messages per worker to consume (0 means run indefinitely)")
  connTimeout = flag.Duration("connection-timeout", 5*time.Second, "Dial timeout")
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

  url := fmt.Sprintf("amqp://%s:%s@%s:%d/%s", *username, *password, *host, *port, *vhost)

  log.Printf("starting consumer: concurrency=%d queue=%s host=%s port=%d vhost=%s user=%s",
    concurrency, *queueName, *host, *port, *vhost, *username)

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

  var wg sync.WaitGroup
  var totalReceived atomic.Int64
  var totalErrors atomic.Int64

  // stats ticker across the whole process
  ctxAll, cancelAll := context.WithCancel(context.Background())
  defer cancelAll()
  statsTicker := time.NewTicker(10 * time.Second)
  defer statsTicker.Stop()
  go func() {
    for {
      select {
      case <-ctxAll.Done():
        return
      case <-statsTicker.C:
        log.Printf("stats: received=%d errors=%d goroutines=%d",
          totalReceived.Load(), totalErrors.Load(), runtime.NumGoroutine())
      }
    }
  }()

  // Spawn one connection per worker and within each worker start consumers
  // for ALL queue partitions. Total consumers ~= queuePartitions * GOMAXPROCS.
  for i := 0; i < concurrency; i++ {
    wg.Add(1)
    go func(workerID int) {
      defer wg.Done()

      // Worker-local context to stop when per-worker message limit is reached
      ctx, cancel := context.WithCancel(ctxAll)
      defer cancel()

      var consumed atomic.Int64
      // One shared connection per worker; channels per partition goroutine.
      var (
        conn   *amqp.Connection
        connMu sync.RWMutex
      )
      connect := func() error {
        dialer := amqp.DefaultDial(*connTimeout)
        cfg := amqp.Config{Dial: dialer, Heartbeat: 0, Locale: "en_US"}
        c, err := amqp.DialConfig(url, cfg)
        if err != nil {
          return err
        }
        connMu.Lock()
        if conn != nil {
          _ = conn.Close()
        }
        conn = c
        connMu.Unlock()
        return nil
      }
      if err := connect(); err != nil {
        totalErrors.Add(1)
        log.Printf("worker %d: initial dial error: %v", workerID, err)
      }

      // Helper to determine if worker should stop due to message limit
      shouldStop := func() bool {
        if *messages <= 0 {
          return false
        }
        return consumed.Load() >= *messages
      }

      // Build the list of queues this worker is responsible for.
      // Each worker handles ALL partitions, so overall we get GOMAXPROCS consumers per partition.
      var queues []string
      if *queueParts <= 1 {
        queues = []string{*queueName}
      } else {
        for part := 0; part < *queueParts; part++ {
          queues = append(queues, fmt.Sprintf("%s_%d", *queueName, part))
        }
      }

      var wgParts sync.WaitGroup
      for _, qn := range queues {
        qName := qn
        wgParts.Add(1)
        go func() {
          defer wgParts.Done()

          for !shouldStop() {
            // Open a channel on the shared worker connection
            connMu.RLock()
            c := conn
            connMu.RUnlock()
            if c == nil {
              if err := connect(); err != nil {
                totalErrors.Add(1)
                log.Printf("worker %d: reconnect error: %v", workerID, err)
                time.Sleep(500 * time.Millisecond)
              }
              continue
            }

            ch, err := c.Channel()
            if err != nil {
              totalErrors.Add(1)
              log.Printf("worker %d: channel error: %v", workerID, err)
              _ = c.Close()
              if err := connect(); err != nil {
                totalErrors.Add(1)
                log.Printf("worker %d: reconnect error: %v", workerID, err)
              }
              time.Sleep(500 * time.Millisecond)
              continue
            }
            // Best-effort fair dispatch
            _ = ch.Qos(250, 0, false)

            deliveries, err := ch.Consume(qName, "", false, false, false, false, nil)
            if err != nil {
              totalErrors.Add(1)
              log.Printf("worker %d: consume start error on %s: %v", workerID, qName, err)
              _ = ch.Close()
              time.Sleep(500 * time.Millisecond)
              continue
            }

            for {
              select {
              case <-ctx.Done():
                _ = ch.Close()
                return
              case msg, ok := <-deliveries:
                if !ok {
                  // server closed channel/connection; break to recreate
                  _ = ch.Close()
                  goto recreate
                }
                if err := msg.Ack(false); err != nil {
                  totalErrors.Add(1)
                  log.Printf("worker %d: ack error on %s: %v", workerID, qName, err)
                  _ = ch.Close()
                  time.Sleep(100 * time.Millisecond)
                  goto recreate
                }

                total := totalReceived.Add(1)
                if newVal := consumed.Add(1); newVal%1000 == 0 {
                  log.Printf("worker %d: consumed %d messages from %s (total=%d)", workerID, newVal, qName, total)
                }
                if shouldStop() {
                  _ = ch.Close()
                  cancel()
                  return
                }
              }
            }
          recreate:
            // loop to recreate channel/consumer
            if shouldStop() {
              return
            }
          }
        }()
      }

      wgParts.Wait()
      connMu.RLock()
      if conn != nil { _ = conn.Close() }
      connMu.RUnlock()
    }(i)
  }

  wg.Wait()
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


