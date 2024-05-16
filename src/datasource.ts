import { lastValueFrom, Observable } from 'rxjs';
import { 
    DataSourceInstanceSettings,
    DataQueryRequest, 
    DataQueryResponse, 
    MetricFindValue,
    ScopedVars,
    TimeRange
} from '@grafana/data';
import {
    DataSourceWithBackend, 
    getTemplateSrv,
    frameToMetricFindValue
} from '@grafana/runtime';
import { MongoDBDataSourceOptions, MongoDBQuery, MongoDBQueryType, MongoDBVariableQuery } from './types';
import { format } from 'date-fns'; // 使用 date-fns 进行日期格式化

export class DataSource extends DataSourceWithBackend<MongoDBQuery, MongoDBDataSourceOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<MongoDBDataSourceOptions>) {
    super(instanceSettings);
  }

  applyTemplateVariables(query: MongoDBQuery, scopedVars: ScopedVars): Record<string, any> {
    const templateSrv = getTemplateSrv();
    const processedQuery = {
      ...query,
      database: query.database ? templateSrv.replace(query.database, scopedVars) : '',
      collection: query.collection ? templateSrv.replace(query.collection, scopedVars) : '',
      aggregation: query.aggregation ? templateSrv.replace(query.aggregation, scopedVars, 'json') : ''
    };

    return processedQuery;
  }

  replaceCustomFunctions(aggregation: string, timeRange: TimeRange): string {
    // 匹配所有自定义函数，包括可选的日期格式化字符串
    const customFuncRegex = /\{\{\s*__(time_from|time_to):date(?::([^\s\}]+))?\s*\}\}/g;

    return aggregation.replace(customFuncRegex, (match, p1, p2) => {
      const timeValue = p1 === 'time_from' ? timeRange.from.toDate() : timeRange.to.toDate();

      if (!p2 || p2 === 'iso') {
        // 处理 `__time_from:date` 或 `__time_to:date` 或 `__time_from:date:iso` 的情况
        return timeValue.toISOString();
      }

      switch (p2) {
        case 'toObjectId':
          return this.timeToObjectId(timeValue);
        case 'seconds':
          return Math.floor(timeValue.valueOf() / 1000).toString();
        case 'milliseconds':
          return timeValue.valueOf().toString();
        default:
          // 处理自定义日期格式化字符串
          return format(timeValue, p2);
      }
    });
  }

  // Helper function to convert time to MongoDB ObjectId format
  timeToObjectId(time: Date): string {
    const timestamp = Math.floor(time.valueOf() / 1000).toString(16);
    return timestamp + "0000000000000000";
  }

  query(request: DataQueryRequest<MongoDBQuery>): Observable<DataQueryResponse> {
    const timeRange = request.range;
    const processedTargets = request.targets.map(target => {
      const processedQuery = this.applyTemplateVariables(target, request.scopedVars);
      processedQuery.aggregation = this.replaceCustomFunctions(processedQuery.aggregation, timeRange);
      return processedQuery;
    });

    const modifiedRequest: DataQueryRequest<MongoDBQuery> = {
      ...request,
      targets: processedTargets as MongoDBQuery[],
    };

    return super.query(modifiedRequest);
  }

  async metricFindQuery(query: MongoDBVariableQuery, options?: any): Promise<MetricFindValue[]> {
    const target: Partial<MongoDBQuery> = {
        refId: 'metricFindQuery',
        database: query.database,
        collection: query.collection,
        queryType: MongoDBQueryType.Table,
        timestampField: "",
        timestampFormat: "",
        labelFields: [],
        valueFields: [ query.fieldName ],
        valueFieldTypes: [ query.fieldType ],
        aggregation: query.aggregation,
        autoTimeBound: false,
        autoTimeSort: false,
        schemaInference: false,
        schemaInferenceDepth: 0,
    };

    let dataQuery = {
        ...options,
        targets: [target]
    };
    let dataQueryRequest = dataQuery as DataQueryRequest<MongoDBQuery>;

    return lastValueFrom(
        this.query(dataQueryRequest)
    ).then((rsp) => {
        if (rsp.error) {
            throw new Error(rsp.error.message);
        }
        if (rsp.data?.length) {
          return frameToMetricFindValue(rsp.data[0]);
        }
        return [];
    });
  }
}
