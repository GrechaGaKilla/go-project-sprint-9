package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Generator генерирует числа.
func Generator(ctx context.Context, ch chan<- int64, fn func(int64)) {
	var i int64 = 1
	for {
		select {
		case <-ctx.Done():
			return
		case ch <- i:
			fn(i)
			i++
		}
	}
}

// Worker обрабатывает числа.
func Worker(wg *sync.WaitGroup, in <-chan int64, out chan<- int64) {
	defer wg.Done()
	for v := range in {
		time.Sleep(10 * time.Millisecond)
		out <- v
	}
}

func main() {
	chIn := make(chan int64)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var atomicInputSum int64
	var atomicInputCount int64

	var genWg sync.WaitGroup
	genWg.Add(1)

	go func() {
		defer genWg.Done()
		Generator(ctx, chIn, func(i int64) {
			atomic.AddInt64(&atomicInputSum, i)
			atomic.AddInt64(&atomicInputCount, 1)
		})
	}()

	const NumOut = 5
	outs := make([]chan int64, NumOut)
	var wg sync.WaitGroup

	for i := 0; i < NumOut; i++ {
		outs[i] = make(chan int64)
		wg.Add(1)
		go Worker(&wg, chIn, outs[i])
	}

	amounts := make([]int64, NumOut)
	chOut := make(chan int64, NumOut)

	var outWg sync.WaitGroup

	for i := 0; i < NumOut; i++ {
		outWg.Add(1)
		go func(index int) {
			defer outWg.Done()
			for v := range outs[index] {
				amounts[index]++
				chOut <- v
			}
		}(i)
	}

	var count int64
	var sum int64
	go func() {
		for v := range chOut {
			sum += v
			count++
		}
	}()

	genWg.Wait()
	close(chIn)

	wg.Wait()
	for i := 0; i < NumOut; i++ {
		close(outs[i])
	}

	outWg.Wait()
	close(chOut)

	inputSum := atomicInputSum
	inputCount := atomicInputCount
	fmt.Println("Количество чисел", inputCount, count)
	fmt.Println("Сумма чисел", inputSum, sum)
	fmt.Println("Разбивка по каналам", amounts)

	if inputSum != sum {
		log.Fatalf("Ошибка: суммы чисел не равны: %d != %d\n", inputSum, sum)
	}
	if inputCount != count {
		log.Fatalf("Ошибка: количество чисел не равно: %d != %d\n", inputCount, count)
	}
	for _, v := range amounts {
		inputCount -= v
	}
	if inputCount != 0 {
		log.Fatalf("Ошибка: разделение чисел по каналам неверное\n")
	}
}
