package ru.surf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;


public class Index {
    //нет модификатора доступа, небезопасная публикация объекта
    TreeMap<String, List<Pointer>> invertedIndex;

    //нет модификатора доступа, небезопасная публикация объекта
    ExecutorService pool;

    public Index(ExecutorService pool) {
        this.pool = pool;
        //лучше указать this
        invertedIndex = new TreeMap<>();
    }

    //по факту метод берёт из папки не только txt, название неподходящее
    public void indexAllTxtInPath(String pathToDir) throws IOException {
        Path of = Path.of(pathToDir);

        //магическая константа
        BlockingQueue<Path> files = new ArrayBlockingQueue<>(2);

        try (Stream<Path> stream = Files.list(of)) {
            stream.forEach(files::add);
        }

        pool.submit(new IndexTask(files));
        pool.submit(new IndexTask(files));
        pool.submit(new IndexTask(files));
    }

    //небезопасная публикация объекта
    public TreeMap<String, List<Pointer>> getInvertedIndex() {
        return invertedIndex;
    }

    //название метода нарушает java notation
    //небезопасная публикация объекта
    public List<Pointer> GetRelevantDocuments(String term) {
        return invertedIndex.get(term);
    }

    public Optional<Pointer> getMostRelevantDocument(String term) {
        return invertedIndex.get(term).stream().max(Comparator.comparing(o -> o.count));
    }

    static class Pointer {
        private Integer count;
        private String filePath;

        public Pointer(Integer count, String filePath) {
            this.count = count;
            this.filePath = filePath;
        }

        @Override
        public String toString() {
            return "{" + "count=" + count + ", filePath='" + filePath + '\'' + '}';
        }
    }

    class IndexTask implements Runnable {

        private final BlockingQueue<Path> queue;

        public IndexTask(BlockingQueue<Path> queue) {
            this.queue = queue;
        }

        //метод берёт все слова в файле и считает сколько из них идентичны пути до этого файла
        //добавляет данные об этом в invertedIndex
        @Override
        public void run() {
            try {
                Path take = queue.take();
                List<String> strings = Files.readAllLines(take);

                //сложные лямбда выражения лучше выносить в отдельные методы
                strings.stream().flatMap(str -> Stream.of(str.split(" "))).forEach(word -> invertedIndex.compute(word, (k, v) -> {
                    //else можно опустить
                    if (v == null) return List.of(new Pointer(1, take.toString()));
                    else {
                        ArrayList<Pointer> pointers = new ArrayList<>();

                        //сложные условия лучше выносить в отдельный метод
                        //если условие true то выполнение можно заканчивать, т.к. следующий forEach ничего не сделает
                        if (v.stream().noneMatch(pointer -> pointer.filePath.equals(take.toString()))) {
                            pointers.add(new Pointer(1, take.toString()));
                        }

                        v.forEach(pointer -> {
                            if (pointer.filePath.equals(take.toString())) {
                                pointer.count = pointer.count + 1;
                            }
                        });

                        pointers.addAll(v);

                        return pointers;
                    }

                }));

            } catch (InterruptedException | IOException e) {
                //надо бы залогировать
                throw new RuntimeException();
            }
        }
    }
}