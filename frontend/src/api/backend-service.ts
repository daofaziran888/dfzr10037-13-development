import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ===== 批量导入 · 多组归并 =====
export type BatchCandidate = {
  key: string
  type: string
  group_id: string
  line_no: number
}

export type EdgeRow = {
  group_id: string
  group_name?: string
  line_no: number
  edge_key: string
  relation: string
  source: string
  target: string
  source_type?: string | null
  target_type?: string | null
  reason?: string
  pending_endpoints?: { key: string; reason: string }[]
  built?: boolean
  buildable?: boolean
  [field: string]: unknown
}

export type BatchGroup = {
  group_id: string
  name: string
  来源行数: number
  实体行数: number
  边行数: number
  已建立边: EdgeRow[]
  可建立边: EdgeRow[]
  待定边: EdgeRow[]
  待定端点: PendingEndpoint[]
  source_rows: Record<string, unknown>[]
}

export type PendingEndpoint = {
  key: string
  status: string
  reason: string
  candidates?: BatchCandidate[]
  edges?: { group_id: string; line_no: number; edge_key: string }[]
}

export type BatchDetail = {
  ok: boolean
  batch_id: number
  id: number
  任务编号: string
  数据文件: string
  目标图谱: string
  status: string
  来源行数: number
  已建立边数: number
  可建立边数: number
  待定边数: number
  待定端点数: number
  groups: BatchGroup[]
  候选类型: BatchCandidate[]
  determined_entities: Record<string, { key: string; type: string; source: string }>
  已建立边: Record<string, unknown>[]
  能继续领的行: EdgeRow[]
  待定边: EdgeRow[]
  待定端点: PendingEndpoint[]
  dispatches: Record<string, unknown>[]
}

export type BatchSummary = {
  id: number
  任务编号: string
  数据文件: string
  目标图谱: string
  status: string
  来源行数: number
  已建立边数: number
  可建立边数: number
  待定边数: number
}

async function jsonOrThrow<T>(response: Response, fallback: string): Promise<T> {
  const payload = await response.json().catch(() => null)
  if (!response.ok) {
    const detail = (payload as { detail?: string } | null)?.detail
    throw new Error(detail || fallback)
  }
  return payload as T
}

export async function listBatches(): Promise<BatchSummary[]> {
  const response = await request('/api/batch_load/batches')
  const payload = await jsonOrThrow<{ items: BatchSummary[] }>(response, '批量归并清单读取失败')
  return payload.items
}

export async function getBatch(batchId: number): Promise<BatchDetail> {
  const response = await request(`/api/batch_load/batches/${batchId}`)
  return await jsonOrThrow<BatchDetail>(response, '批量处理清单读取失败')
}

export async function createBatch(
  targetGraph: string,
  groups: unknown[],
): Promise<BatchDetail> {
  const response = await request('/api/batch_load/batches', {
    method: 'POST',
    body: JSON.stringify({ target_graph: targetGraph, groups }),
  })
  return await jsonOrThrow<BatchDetail>(response, '分批原始数据登记失败')
}

export async function submitGroups(
  batchId: number,
  groups: unknown[],
): Promise<{ ok: boolean; message: string; batch: BatchDetail }> {
  const response = await request(`/api/batch_load/batches/${batchId}/groups`, {
    method: 'POST',
    body: JSON.stringify({ groups }),
  })
  return await jsonOrThrow(response, '原始组写入失败')
}

export async function getRowset(batchId: number): Promise<Record<string, unknown>> {
  const response = await request(`/api/batch_load/batches/${batchId}/rowset`)
  return await jsonOrThrow(response, '分组可构建行集读取失败')
}

export async function dispatchGroups(
  batchId: number,
  groupIds: string[],
  remark = '',
): Promise<Record<string, unknown>> {
  const response = await request(`/api/batch_load/batches/${batchId}/dispatch`, {
    method: 'POST',
    body: JSON.stringify({ group_ids: groupIds, remark }),
  })
  return await jsonOrThrow(response, '局部派发失败')
}

export async function resolveCandidate(
  batchId: number,
  key: string,
  type: string,
  groupId?: string,
  lineNo?: number,
): Promise<Record<string, unknown>> {
  const response = await request(`/api/batch_load/batches/${batchId}/resolutions`, {
    method: 'POST',
    body: JSON.stringify({ key, type, group_id: groupId, line_no: lineNo }),
  })
  return await jsonOrThrow(response, '候选类型纠正失败')
}

export async function getBuildResults(batchId: number): Promise<Record<string, unknown>> {
  const response = await request(`/api/batch_load/batches/${batchId}/results`)
  return await jsonOrThrow(response, '构建结果读取失败')
}

export async function getClaimable(batchId: number): Promise<Record<string, unknown>> {
  const response = await request(`/api/batch_load/batches/${batchId}/claimable`)
  return await jsonOrThrow(response, '可继续领用行读取失败')
}
