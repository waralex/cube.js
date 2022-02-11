use std::env;

use async_trait::async_trait;
use comfy_table::{Cell, Table};
use cubesql::config::Config;
use mysql_async::{prelude::*, Opts, Pool, QueryResult, TextProtocol};
use portpicker::pick_unused_port;

use super::basic::{AsyncTestSuite, RunResult};

#[derive(Debug)]
pub struct MySqlIntegrationTestSuite {
    pool: Pool,
}

impl MySqlIntegrationTestSuite {
    pub(crate) async fn before_all() -> Box<Self> {
        let mut env_defined = false;

        if let Ok(testing_cube_token) = env::var("CUBESQL_TESTING_CUBE_TOKEN".to_string()) {
            env::set_var("CUBESQL_CUBE_TOKEN", testing_cube_token);

            env_defined = true;
        };

        if let Ok(testing_cube_url) = env::var("CUBESQL_TESTING_CUBE_URL".to_string()) {
            env::set_var("CUBESQL_CUBE_URL", testing_cube_url);
        } else {
            env_defined = false;
        };

        if !env_defined {
            println!("Testing variables are not defined, passing....");
            panic!();
        };

        let random_port = pick_unused_port().expect("No ports free");

        tokio::spawn(async move {
            println!("Before all tests");

            let config = Config::default();
            let config = config.update_config(|mut c| {
                c.bind_address = Some(format!("0.0.0.0:{}", random_port));

                c
            });

            let services = config.configure().await;
            services.wait_processing_loops().await.unwrap();
        });

        let url = format!("mysql://root:password@localhost:{}/db", random_port);
        let pool = Pool::new(Opts::from_url(&url).unwrap());

        Box::new(MySqlIntegrationTestSuite { pool })
    }

    async fn print_query_result<'a>(
        &self,
        res: &mut QueryResult<'a, 'static, TextProtocol>,
    ) -> String {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");

        let mut header = vec![];
        for column in res.columns_ref().into_iter() {
            header.push(Cell::new(column.name_str()));
        }
        table.set_header(header);

        res.for_each(|row| {
            let values: Vec<String> = row
                .unwrap()
                .into_iter()
                .map(|v| match v {
                    mysql_async::Value::NULL => "NULL".to_string(),
                    mysql_async::Value::Int(n) => n.to_string(),
                    mysql_async::Value::UInt(n) => n.to_string(),
                    mysql_async::Value::Float(n) => n.to_string(),
                    mysql_async::Value::Double(n) => n.to_string(),
                    mysql_async::Value::Bytes(n) => String::from_utf8(n).unwrap(),
                    _ => unimplemented!(),
                })
                .collect();
            table.add_row(values);
        })
        .await
        .unwrap();

        table.trim_fmt()
    }

    async fn test_use(&self) -> RunResult {
        let mut conn = self.pool.get_conn().await.unwrap();

        {
            let mut response = conn.query_iter("SELECT database()").await.unwrap();
            insta::assert_snapshot!(self.print_query_result(&mut response).await);
        }

        {
            conn.query_iter("USE `information_schema`").await.unwrap();
        }

        let mut response = conn.query_iter("SELECT database()").await.unwrap();
        insta::assert_snapshot!(self.print_query_result(&mut response).await);

        Ok(())
    }

    async fn test_execute_query(&self, query: String) -> RunResult {
        let mut conn = self.pool.get_conn().await.unwrap();
        let mut response = conn.query_iter(query.clone()).await.unwrap();
        insta::assert_snapshot!(query, self.print_query_result(&mut response).await);

        Ok(())
    }
}

#[async_trait]
impl AsyncTestSuite for MySqlIntegrationTestSuite {
    async fn after_all(&mut self) -> RunResult {
        todo!()
    }

    async fn run(&mut self) -> RunResult {
        self.test_use().await?;
        self.test_execute_query("SELECT COUNT(*), status FROM Orders".to_string())
            .await?;
        self.test_execute_query(
            "SELECT COUNT(*), status, createdAt FROM Orders ORDER BY createdAt".to_string(),
        )
        .await?;
        self.test_execute_query(
            "SELECT COUNT(*), status, DATE_TRUNC('month', createdAt) FROM Orders ORDER BY createdAt".to_string(),
        )
        .await?;
        self.test_execute_query(
            "SELECT COUNT(*), status, DATE_TRUNC('quarter', createdAt) FROM Orders ORDER BY createdAt".to_string(),
        )
        .await?;

        Ok(())
    }
}
