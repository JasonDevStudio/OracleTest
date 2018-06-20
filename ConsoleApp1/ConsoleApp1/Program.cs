using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Oracle.DataAccess.Client;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            var txt = string.Empty;

            try
            {
                Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss")} 内存表开始循环组装数据...");

                var data = new DataTable();
                data.Columns.Add("ID", typeof(decimal));
                data.Columns.Add("LOGID", typeof(decimal));
                data.Columns.Add("PF", typeof(decimal));
                data.Columns.Add("VAL", typeof(double));
                data.Columns.Add("ITEMID", typeof(decimal));
                data.Columns.Add("SITEID", typeof(decimal));

                for (int i = 0; i < 1000000; i++)
                {
                    var row = data.NewRow();
                    row["ID"] = i;
                    row["LOGID"] = i;
                    row["PF"] = i;
                    row["VAL"] = i;
                    row["ITEMID"] = i;
                    row["SITEID"] = i;

                    data.Rows.Add(row);
                }

                Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss")} 内存表组装数据完成...");

                var st =  Stopwatch.StartNew();
                Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss")} 开始往数据库写入数据...");

                var connString = @"Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST= 192.168.0.106)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ORCL)));User Id=HLINK;Password=123456";
                var conn = new OracleConnection(connString);
                conn.Open();

                var ocp = new OracleBulkCopy(conn);
                ocp.BatchSize = 1000000;
                ocp.BulkCopyOptions = OracleBulkCopyOptions.UseInternalTransaction;
                ocp.BulkCopyTimeout = 360000;
                ocp.DestinationTableName = "RESULT_ITEM";

                foreach (DataColumn item in data.Columns)
                {
                    ocp.ColumnMappings.Add(item.ColumnName, item.ColumnName);
                }

                ocp.WriteToServer(data);
                ocp.Close();
                ocp.Dispose();

                st.Stop();
                Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss")} 数据库写入完成,数据量 {data.Rows.Count},耗时[{st.Elapsed.TotalSeconds}]...");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            finally
            {
                do
                {
                    txt = Console.ReadLine();
                } while (txt != "EXIT");
            }
        }
    }
}
