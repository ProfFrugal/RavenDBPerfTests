using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

using Raven.Client.Documents;

namespace TestRavenDB
{
    internal class Program
    {
        public class Employee
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public string Title { get; set; }
        }

        static void Main(string[] args)
        {
            string cmd = "query";

            int repetition = 50;

            foreach (var a in args)
            {
                if (int.TryParse(a, out int r))
                {
                    repetition = r;
                }
                else
                {
                    cmd = a;
                }
            }

            Console.WriteLine("Command: {0} {1}", cmd, repetition);

            var store = new DocumentStore
            {
                Urls = new[] { "http://localhost:8080" },
                Database = "Northwind"
            };

            store.Initialize();

            switch (cmd)
            {
                case "add":
                    GenerateData(store, repetition);
                    return;

                case "delete":
                    DeleteData(store);
                    return;

                default:
                    break;
            }

            QueryData(store);

            Thread.Sleep(500);

            DateTime now = DateTime.UtcNow;

            long alloc = GC.GetAllocatedBytesForCurrentThread();

            ValueTuple<int, int> result = (0, 0);

            for (int repeat = 0; repeat < repetition; repeat++)
            {
                result = QueryData(store);
            }

            double totalTime = (DateTime.UtcNow - now).TotalMilliseconds;
            int totalItem = result.Item2 * repetition;

            Console.WriteLine("{0:N2} ms, {1:N0} / {2:N0} x {3}",
                totalTime, result.Item1, result.Item2, repetition);
            Console.WriteLine("{0:N2} μs per item", totalTime * 1000 / totalItem);

            alloc = GC.GetAllocatedBytesForCurrentThread() - alloc;

            Console.WriteLine("Allocation: {0:N0} bytes, {1:N2} per item", alloc, alloc / totalItem);
            Console.WriteLine("Memory usage: {0:N2} mb", GC.GetTotalMemory(false) / 1024 / 1024.0);
        }

        public static ValueTuple<int, int> QueryData(DocumentStore store)
        {
            int total = 0;
            int count = 0;

            using (var session = store.OpenSession())
            {
                var employees = session.Query<Employee>();

                foreach (var e in employees)
                {
                    total++;
                    string last = e.LastName;

                    if (last[last.Length - 1] == '0')
                    {
                        count++;
                    }
                }
            }

            return new ValueTuple<int, int>(count, total);
        }

        public static void DeleteData(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                DateTime now = DateTime.Now;

                int count = 0;

                var employees = session.Query<Employee>();

                foreach (var e in employees)
                {
                    session.Delete(e);

                    count++;
                }

                session.SaveChanges();

                var duration = DateTime.Now - now;

                Console.WriteLine("{0:N0} records deleted in {1} ms", count, duration.TotalMilliseconds);
            }
        }

        public static void GenerateData(DocumentStore store, int count = 10000)
        {
            using (var session = store.OpenSession())
            {
                for (int i = 0; i < count; i++)
                {
                    var employee = new Employee
                    {
                        FirstName = "John" + (i % 1000),
                        LastName = "Doe",
                        Title = "Software Developer"
                    };

                    session.Store(employee);
                }

                session.SaveChanges();
            }
        }

    }
}
