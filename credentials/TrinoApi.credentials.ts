import type { ICredentialType, INodeProperties } from 'n8n-workflow';

export class TrinoApi implements ICredentialType {
	name = 'trinoApi';
	displayName = 'Trino API';
	documentationUrl = 'https://trino.io/docs/current/client/jdbc.html';
	properties: INodeProperties[] = [
		{
			displayName: 'HTTP over SSL',
			name: 'ssl',
			type: 'boolean',
			description: 'Whether to use https protocol for the connection',
			default: false,
			required: true,
		},
		{
			displayName: 'Host',
			name: 'host',
			type: 'string',
			default: '',
			placeholder: 'your.host',
			required: true,
		},
		{
			displayName: 'Port',
			name: 'port',
			type: 'number',
			default: '',
			placeholder: '8080',
			required: true,
		},
		{
			displayName: 'User',
			name: 'user',
			type: 'string',
			default: '',
			placeholder: 'username',
			required: false,
		},
		{
			displayName: 'Password',
			name: 'password',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
			required: false,
		},
		{
			displayName: 'Catalog',
			name: 'catalog',
			type: 'string',
			description: 'The catalog to use for the connection',
			default: '',
			placeholder: 'jmx',
			required: true,
		},
		{
			displayName: 'Schema',
			name: 'schema',
			type: 'string',
			description: 'The schema to use for the connection',
			default: '',
			placeholder: 'information_schema',
			required: true,
		},
		{
			displayName: 'Source',
			name: 'source',
			type: 'string',
			description: 'The source application name',
			default: 'n8n',
			placeholder: 'n8n',
			required: true,
		},
		{
			displayName: 'HTTP Headers',
			name: 'httpHeaders',
			type: 'json',
			description: 'Additional HTTP headers to send with requests',
			default: '{}',
			required: false,
		},
		{
			displayName: 'Ignore SSL Issues (Insecure)',
			name: 'ignoreSslIssues',
			type: 'boolean',
			description: 'Whether to connect even if SSL certificate validation is not possible',
			default: true,
			required: true,
		},
	];
}
