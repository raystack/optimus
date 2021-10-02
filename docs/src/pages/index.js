import React from 'react';
import Layout from '@theme/Layout';
import clsx from 'clsx';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Container from '../core/Container';
import GridBlock from '../core/GridBlock';
import useBaseUrl from '@docusaurus/useBaseUrl';

const Hero = () => {
  const { siteConfig } = useDocusaurusContext();
  return (
    <div className="homeHero">
      <div className="logo"><img src={useBaseUrl('img/pattern.svg')} /></div>
      <div className="container banner">
        <div className="row">
          <div className={clsx('col col--5')}>
            <div className="homeTitle">{siteConfig.tagline}</div>
            <small className="homeSubTitle">Optimus is an open source performant workflow orchestrator for data transformation, data modeling, pipelines, and data quality management.</small>
            <a className="button" href="docs/introduction">Documentation</a>
          </div>
          <div className={clsx('col col--1')}></div>
          <div className={clsx('col col--6')}>
            <div className="text--right"><img src={useBaseUrl('img/banner.svg')} /></div>
          </div>
        </div>
      </div>
    </div >
  );
};

export default function Home() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.tagline}
      description="Meteor is an easy-to-use, plugin-driven metadata collection framework to extract data from different sources and sink to any data catalog or store.">
      <Hero />
      <main>
        <Container className="textSection wrapper" background="light">
          <h1>Built for scale</h1>
          <p>
            Optimus is an easy-to-use, reliable, and performant workflow orchestrator
            for data transformation, data modeling, pipelines, and data quality management.
            It enables data analysts and engineers to transform their data by writing simple SQL
            queries and YAML configuration while Optimus handles dependency management, scheduling
            and all other aspects of running transformation jobs at scale.
          </p>
          <GridBlock
            layout="threeColumn"
            contents={[
              {
                title: 'Zero dependency',
                content: (
                  <div>
                    Optimus is written in Go and compiles into a single binary with no external dependencies,
                    and requires a very minimal memory footprint.
                  </div>
                ),
              },
              {
                title: 'Warehouse management',
                content: (
                  <div>
                    Optimus allows you to create and manage your data warehouse
                    tables and views through YAML based configuration.
                  </div>
                ),
              },
              {
                title: 'Extensible',
                content: (
                  <div>
                    With the ease of plugin development build your own plugins.
                    Optimus support Python transformation and allows for writing custom plugins.
                  </div>
                ),
              },
              {
                title: 'CLI',
                content: (
                  <div>
                    Optimus comes with a CLI which allows you to interact with workflows effectively.
                    You can create, run, replay jobs and more.
                  </div>
                ),
              },
              {
                title: 'Proven',
                content: (
                  <div>
                    Battle tested at large scale across multiple companies. Largest deployment runs
                    thousands of workflows on multiple data sources.
                  </div>
                ),
              },
              {
                title: 'Workflows',
                content: (
                  <div>
                    Optimus provides industry-proven workflows using git and REST/GRPC based specification management for data warehouse management.
                  </div>
                ),
              },
            ]}
          />
        </Container>
        <Container className="textSection wrapper" background="dark">
          <h1>Key features</h1>
          <p>
            Optimus is an ETL orchestration tool that helps manage warehouse resources
            and schedule transformation over cron interval. Warehouses like Bigquery
            can be used to create, update, read, delete different types of
            resources(dataset/table/standard view). Similarly, jobs can be SQL transformations
            taking inputs from single/multiple source tables executing over fixed schedule interval.
            Optimus was made from start to be extensible, which is, adding support of different kind
            of warehouses, transformers can be done easily.
          </p>
          <GridBlock
            layout="threeColumn"
            contents={[
              {
                title: 'Scheduling',
                content: (
                  <div>
                    Optimus provides an easy way to schedule your SQL
                    transformation through a YAML based configuration.
                  </div>
                ),
              },
              {
                title: 'Dependency resolution',
                content: (
                  <div>
                    Optimus parses your data transformation queries and
                    builds a dependency graphs automaticaly instead of
                    users defining it in DAGs.
                  </div>
                ),
              },
              {
                title: 'Dry runs',
                content: (
                  <div>
                    Before SQL query is scheduled for transformation,
                    during deployment query will be dry-run to make
                    sure it passes basic sanity checks.
                  </div>
                ),
              },
              {
                title: 'Powerful templating',
                content: (
                  <div>
                    Optimus provides query compile time templating with
                    variables, loop, if statements, macros, etc for allowing
                    users to write complex tranformation logic.
                  </div>
                ),
              },
              {
                title: 'Cross tenant dependency',
                content: (
                  <div>
                    Optimus is a multi-tenant service. With more than two
                    tenants registered Optimus can resolve cross tenant
                    dependencies automatically.
                  </div>
                ),
              },
              {
                title: 'Hooks',
                content: (
                  <div>
                    Optimus provides hooks for post tranformation logic
                    to extend the functionality of the transformation.
                    e,g. You can sink BigQuery tables to Kafka.
                  </div>
                ),
              },
            ]}
          />
        </Container>


        <Container className="textSection wrapper" background="light">
          <h1>Workflow</h1>
          <p>
            With Optimus data teams work directly with the data warehouse
            and data catalogs. Optimus provides a set of workflows which
            can be used to build data transformation pipelines, reporting,
            operational, machine learning workflows.
          </p>
          <div className="row">
            <div className="col col--4">

              <GridBlock
                contents={[
                  {
                    title: 'Develop',
                    content: (
                      <div>
                        Write your specifications in git using Optimus CLI or
                        use Optimus APIs to prgramtically submit specifications
                        through SDK.
                      </div>
                    ),
                  },
                  {
                    title: 'Test',
                    content: (
                      <div>
                        Test your workflows prior to production with linting,
                        dry runs and local execution from your machines.
                      </div>
                    ),
                  },
                  {
                    title: 'Deploy',
                    content: (
                      <div>
                        Deploy your workflows safely with Optimus CLI and APIs
                        to production.
                      </div>
                    ),
                  },
                ]}
              />
            </div>
            <div className="col col--8">
              <img src={useBaseUrl('img/overview.svg')} />
            </div>
          </div>
        </Container>

        {/* <Container className="textSection wrapper" background="light">
          <h1>Trusted by</h1>
          <p>
            Optimus was originally created for the Gojek data processing platform,
            and it has been used, adapted and improved by other teams internally and externally.
          </p>
          <GridBlock className="logos"
            layout="fourColumn"
            contents={[
              {
                content: (
                  <img src={useBaseUrl('users/gojek.png')} />
                ),
              },
              {
                content: (
                  <img src={useBaseUrl('users/midtrans.png')} />
                ),
              },
              {
                content: (
                  <img src={useBaseUrl('users/mapan.png')} />
                ),
              },
              {
                content: (
                  <img src={useBaseUrl('users/moka.png')} />
                ),
              },
            ]}>
          </GridBlock>
        </Container> */}
      </main>
    </Layout >
  );
}
