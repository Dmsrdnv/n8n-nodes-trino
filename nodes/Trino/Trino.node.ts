import {
  IExecuteFunctions,
  IDataObject,
  INodeExecutionData,
  INodeType,
  INodeTypeDescription,
  NodeConnectionType,
  NodeOperationError,
  GenericValue,
} from 'n8n-workflow';
import { BasicAuth, Trino as TrinoClient, QueryResult } from 'trino-client';

interface TrinoColumn {
  name: string;
  type: string;
}

export class Trino implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Trino',
    name: 'trino',
    icon: 'file:trino.svg',
    group: ['database', 'input'],
		parameterPane: 'wide',
		usableAsTool: true,
    version: 1,
    description: 'Execute queries against Trino',
    defaults: { name: 'Trino' },
    inputs: [NodeConnectionType.Main],
    outputs: [NodeConnectionType.Main],
    credentials: [{ name: 'trinoApi', required: true }],
    properties: [
      {
        displayName: 'Query',
        name: 'query',
        type: 'string',
        default: 'SELECT *\nFROM jmx.information_schema.tables\nWHERE 1=1\nORDER BY 1\nLIMIT 5',
        placeholder: "SELECT *\nFROM jmx.information_schema.tables\nWHERE 1=1\nORDER BY 1\nLIMIT 5",
        required: true,
				typeOptions: {
					rows: 15,
				},
      },
      {
        displayName: 'Timeout (Seconds)',
        name: 'timeout',
        type: 'number',
        default: 900,
        description: 'Total time before aborting the query',
        required: true,
      },
    ],
  };

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const creds = (await this.getCredentials('trinoApi')) as IDataObject;
    const query = this.getNodeParameter('query', 0) as string;

    const paramTimeout = this.getNodeParameter('timeout', 0) as number;
    const timeoutSec = paramTimeout > 0 ? paramTimeout : (creds.timeout as number ?? 900);

    const protocol = creds.ssl ? 'https' : 'http';
    const port = creds.port as number;
    if (!port)
      throw new NodeOperationError(
        this.getNode(),
        'Port is required in Trino credentials (creds.port)'
      );

    const server = `${protocol}://${creds.host}:${port}`;

    let iter: AsyncIterableIterator<QueryResult> | undefined;

    try {
      const trinoClient = TrinoClient.create({
        server: server,
        catalog: creds.catalog as string,
        schema: creds.schema as string,
        auth: new BasicAuth(creds.user as string, creds.password as string),
        source: creds.source as string ?? 'n8n',
        ssl: creds.ssl ? { rejectUnauthorized: !creds.ignoreSslIssues } : undefined,
      });

      iter = await trinoClient.query(query);

      const processQuery = async (): Promise<INodeExecutionData[]> => {
        const rows: INodeExecutionData[] = [];
        if (!iter) {
          throw new NodeOperationError(this.getNode(), "Iterator not initialized", { itemIndex: 0 });
        }
        for await (const queryResult of iter) {
          if (queryResult.error) {
            throw new NodeOperationError(
              this.getNode(),
              `Trino Error: ${queryResult.error.message} | Query: ${query}`,
              { description: queryResult.error.message, itemIndex: 0 }
            );
          }

          if (queryResult.data && queryResult.columns) {
            const cols = queryResult.columns.map((c: TrinoColumn) => c.name);
            for (const r of queryResult.data) {
              const obj: IDataObject = {};
              (r as unknown[]).forEach((val: unknown, i: number) => (obj[cols[i]] = val as GenericValue));
              rows.push({ json: obj });
            }
          }
        }
        return rows;
      };

      const timer = new Promise<never>((_, reject) =>
        setTimeout(() =>
          reject(new NodeOperationError(
            this.getNode(),
            `Query exceeded overall timeout of ${timeoutSec}s`
          )),
          timeoutSec * 1000
        )
      );

      const rows = await Promise.race([processQuery(), timer]);

      return this.prepareOutputData(rows);
    } catch (error: any) {
      if (error instanceof NodeOperationError) {
        throw error;
      }
      throw new NodeOperationError(this.getNode(), error, { itemIndex: 0 });
    } finally {
      if (iter && iter.return) {
        await iter.return();
      }
    }
  }
}
