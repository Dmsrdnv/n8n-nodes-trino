import {
  IExecuteFunctions,
  IDataObject,
  INodeExecutionData,
  INodeType,
  INodeTypeDescription,
  NodeOperationError,
  GenericValue,
} from 'n8n-workflow';
import { BasicAuth, Trino as TrinoClient, QueryResult, RequestHeaders } from 'trino-client';

interface TrinoColumn {
  name: string;
  type: string;
}

export class Trino implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Trino',
    name: 'trino',
    icon: 'file:trino.svg',
    group: ['transform'],
    parameterPane: 'wide',
    usableAsTool: true,
    version: 1,
    description: 'Execute queries against Trino',
    defaults: { name: 'Trino' },
    inputs: ['main'],
    outputs: ['main'],
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
        description: 'Total time before aborting the query and the connection',
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
    if (!port) {
      throw new NodeOperationError(this.getNode(), 'Port is required in Trino credentials (creds.port)');
    }
    const server = `${protocol}://${creds.host}:${port}`;
    const catalog = creds.catalog as string;
    const schema = creds.schema as string;
    const user = creds.user as string;
    const source = creds.source as string ?? 'n8n';
    const rawHttpHeaders = creds.httpHeaders;
    let httpHeaders: RequestHeaders = {};

    if (typeof rawHttpHeaders === 'string' && rawHttpHeaders.trim()) {
      try {
        const parsedHeaders = JSON.parse(rawHttpHeaders);
        if (typeof parsedHeaders === 'object' && parsedHeaders !== null && !Array.isArray(parsedHeaders)) {
          httpHeaders = parsedHeaders;
        } else {
          this.logger.warn(`n8n-node-trino: 'httpHeaders' parsed from string is not a valid object. Ignoring.`);
        }
      } catch (e: any) {
        this.logger.warn(`n8n-node-trino: Failed to parse 'httpHeaders' string as JSON: ${e.message}. Ignoring.`);
      }
    } else if (typeof rawHttpHeaders === 'object' && rawHttpHeaders !== null && !Array.isArray(rawHttpHeaders)) {
      httpHeaders = rawHttpHeaders as RequestHeaders;
    } else if (rawHttpHeaders) {
      this.logger.warn(`n8n-node-trino: 'httpHeaders' has an unexpected type (${typeof rawHttpHeaders}). Ignoring.`);
    }

    let iter: AsyncIterableIterator<QueryResult> | undefined;
    let timerId: NodeJS.Timeout | undefined;
    let operationCancelled = false;

    const connectionContext = `n8n-node-trino: Context: server=${server}, user=${user}, catalog=${catalog}, schema=${schema}, source=${source}, extraHeaders=${JSON.stringify(httpHeaders)}`;

    try {
      // Set up timeout promise first, before starting the operation
      const timerPromise = new Promise<never>((_, reject) => {
        timerId = setTimeout(() => {
          operationCancelled = true;
          const timedOutIter = iter;
          timerId = undefined;
          this.logger.warn(`n8n-node-trino: Operation exceeded timeout of ${timeoutSec}s on ${server}. Aborting.`);
          if (timedOutIter && timedOutIter.return) {
            timedOutIter.return().catch(e => this.logger.error(`n8n-node-trino: Error cancelling iterator on timeout for ${server}: ${e.message}`));
          }
          reject(new NodeOperationError(this.getNode(), `Operation exceeded timeout of ${timeoutSec}s`, { itemIndex: 0 }));
        }, timeoutSec * 1000);
      });

      const operationPromise = (async (): Promise<INodeExecutionData[][]> => {
        this.logger.debug(`n8n-node-trino: Attempting connection and query to ${server}`);

        const trinoClient = TrinoClient.create({
          server,
          catalog,
          schema,
          auth: new BasicAuth(user, creds.password as string),
          source,
          ssl: creds.ssl ? { rejectUnauthorized: !creds.ignoreSslIssues } : undefined,
          extraHeaders: httpHeaders,
        });
        this.logger.debug(`n8n-node-trino: Client initialized for ${server}`);

        this.logger.debug(`n8n-node-trino: Preparing to execute query with context: server=${server}, user=${user}, catalog=${catalog}, schema=${schema}, source=${source}, extraHeaders=${JSON.stringify(httpHeaders)}`);
        iter = await trinoClient.query(query);
        this.logger.info(`n8n-node-trino: Executing query: ${query}`);

        const rows: INodeExecutionData[] = [];
        if (!iter) throw new NodeOperationError(this.getNode(), 'Iterator not initialized', { itemIndex: 0 });

        for await (const result of iter) {
          if (operationCancelled) {
            this.logger.warn('n8n-node-trino: Timeout occurred during result iteration, stopping processing.');
            break;
          }
          if (result.error) {
            throw new NodeOperationError(this.getNode(), `Trino query Error: ${result.error.message}`, { description: result.error.message, itemIndex: 0 });
          }
          if (result.data && result.columns) {
            const cols = result.columns.map((c: TrinoColumn) => c.name);
            for (const r of result.data) {
              const obj: IDataObject = {};
              (r as unknown[]).forEach((val, i) => (obj[cols[i]] = val as GenericValue));
              rows.push({ json: obj });
            }
          }
        }
        if (!operationCancelled) {
            return this.prepareOutputData(rows);
        } else {
            return [];
        }
      })();

      const result = await Promise.race([operationPromise, timerPromise]);
      if (timerId) {
          clearTimeout(timerId);
      }
      return result;

    } catch (error: any) {
      if (timerId) {
        clearTimeout(timerId);
      }
      this.logger.error(`n8n-node-trino: Error during operation for ${server}: ${error.message}`);

      const errorIter = iter;
      if (errorIter && errorIter.return && !(error instanceof NodeOperationError && error.message.includes('Operation exceeded timeout'))) {
         await errorIter.return().catch(e => this.logger.error(`n8n-node-trino: Error during final catch cleanup for ${server}: ${e.message}`));
      }

      const detailedErrorMessage = `${error.message || 'Unknown error'}. ${connectionContext}`;
      const errorOptions = {
        itemIndex: (error as any)?.itemIndex ?? 0,
        description: typeof (error as any)?.description === 'string' ? (error as any).description : undefined,
        cause: error instanceof Error ? error : undefined
      };

      throw new NodeOperationError(this.getNode(), detailedErrorMessage, errorOptions);
    }
  }
}
