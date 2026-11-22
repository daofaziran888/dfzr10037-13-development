<template>
  <section class="merge-panel" data-testid="batch-merge-panel">
    <header class="merge-head">
      <div>
        <h3>多组归并 · 分组可构建行集</h3>
        <p class="merge-desc">
          多组原始数据映射到同一张图谱：同一设备在不同组里类型相异时保留全部候选，
          端点未定的边不进入构建；派发后构建结果回挂到本批量处理清单。
        </p>
      </div>
      <label class="merge-select">
        <span>批量处理清单</span>
        <select v-model.number="selectedBatchId" data-testid="batch-select" @change="loadBatch">
          <option v-for="item in batches" :key="item.id" :value="item.id">
            {{ item.任务编号 }} · {{ item.数据文件 }}（{{ item.status }}）
          </option>
        </select>
      </label>
    </header>

    <p v-if="mergeError" class="error-text" data-testid="merge-error">{{ mergeError }}</p>
    <p v-if="mergeNotice" class="merge-notice" data-testid="merge-notice">{{ mergeNotice }}</p>

    <template v-if="detail">
      <div class="stat-row">
        <article class="stat-card" data-testid="stat-built">
          <span class="stat-label">已建立边</span>
          <strong class="stat-value">{{ detail.已建立边数 }}</strong>
        </article>
        <article class="stat-card" data-testid="stat-claimable">
          <span class="stat-label">可建立边（未派发）</span>
          <strong class="stat-value">{{ detail.可建立边数 }}</strong>
        </article>
        <article class="stat-card" data-testid="stat-pending">
          <span class="stat-label">待定边</span>
          <strong class="stat-value">{{ detail.待定边数 }}</strong>
        </article>
        <article class="stat-card" data-testid="stat-endpoints">
          <span class="stat-label">待定端点</span>
          <strong class="stat-value">{{ detail.待定端点数 }}</strong>
        </article>
      </div>

      <div class="merge-columns">
        <article class="merge-card">
          <h4>累计候选类型（保留原始组与行号）</h4>
          <table class="data-table compact" data-testid="candidate-table">
            <thead>
              <tr><th>实体键</th><th>候选类型</th><th>原始组</th><th>行号</th><th>状态</th></tr>
            </thead>
            <tbody>
              <tr
                v-for="candidate in detail.候选类型"
                :key="`${candidate.group_id}-${candidate.line_no}-${candidate.type}`"
                :data-testid="`candidate-${candidate.group_id}-${candidate.line_no}`"
              >
                <td>{{ candidate.key }}</td>
                <td>{{ candidate.type }}</td>
                <td>{{ candidate.group_id }}</td>
                <td>{{ candidate.line_no }}</td>
                <td>
                  <span v-if="isDetermined(candidate.key)" class="tag ok">
                    已确定：{{ detail.determined_entities[candidate.key].type }}
                  </span>
                  <span v-else class="tag warn">待定</span>
                </td>
              </tr>
            </tbody>
          </table>
        </article>

        <article class="merge-card">
          <h4>端点类型纠正</h4>
          <p class="merge-hint">
            同键候选类型不一致时不会认作重复；给出纠正类型后相关边重新进入可领行。
          </p>
          <form class="analysis-form" data-testid="resolve-form" @submit.prevent="submitResolution">
            <label>
              <span>实体键</span>
              <input v-model.trim="resolveKey" data-testid="resolve-key" required placeholder="如 D18" />
            </label>
            <label>
              <span>纠正类型</span>
              <input v-model.trim="resolveType" data-testid="resolve-type" required placeholder="如 传感器" />
            </label>
            <button class="btn primary" data-testid="resolve-submit" type="submit" :disabled="busy">
              提交纠正
            </button>
          </form>

          <div v-if="detail.待定端点.length" class="pending-endpoints" data-testid="pending-endpoints">
            <div v-for="endpoint in detail.待定端点" :key="endpoint.key" class="pending-endpoint">
              <strong>{{ endpoint.key }}</strong>
              <span class="tag warn">{{ endpoint.reason }}</span>
              <span class="muted">
                候选：
                <em v-for="c in endpoint.candidates" :key="`${c.group_id}-${c.line_no}`">
                  {{ c.type }}@{{ c.group_id }}#{{ c.line_no }}
                </em>
              </span>
              <button
                v-for="c in endpoint.candidates"
                :key="`fix-${c.group_id}-${c.line_no}`"
                class="link"
                type="button"
                :data-testid="`quick-resolve-${c.group_id}-${c.line_no}`"
                @click="quickResolve(c.key, c.type)"
              >
                采用 {{ c.type }}（{{ c.group_id }}#{{ c.line_no }}）
              </button>
            </div>
          </div>
        </article>
      </div>

      <article class="merge-card">
        <div class="merge-card-head">
          <h4>按组核对：可建立边 / 待定端点</h4>
          <div class="merge-actions">
            <button class="btn" type="button" data-testid="select-all-groups" @click="toggleAll">
              {{ allSelected ? '取消全选' : '全选分组' }}
            </button>
            <button
              class="btn primary"
              type="button"
              data-testid="dispatch-button"
              :disabled="busy"
              @click="submitDispatch"
            >
              局部派发到图谱构建
            </button>
          </div>
        </div>
        <table class="data-table compact" data-testid="group-table">
          <thead>
            <tr>
              <th>选择</th><th>分组</th><th>来源行</th><th>已建立</th><th>可建立</th><th>待定</th><th>待定端点</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="group in detail.groups" :key="group.group_id" :data-testid="`group-${group.group_id}`">
              <td>
                <input v-model="selectedGroups" type="checkbox" :value="group.group_id" />
              </td>
              <td>{{ group.group_id }} · {{ group.name }}</td>
              <td>{{ group.来源行数 }}</td>
              <td data-testid="group-built">{{ group.已建立边.length }}</td>
              <td data-testid="group-claimable">{{ group.可建立边.length }}</td>
              <td data-testid="group-pending">{{ group.待定边.length }}</td>
              <td>
                <span v-for="endpoint in group.待定端点" :key="endpoint.key" class="tag warn">
                  {{ endpoint.key }}（{{ endpoint.reason }}）
                </span>
                <span v-if="!group.待定端点.length" class="muted">—</span>
              </td>
            </tr>
          </tbody>
        </table>
      </article>

      <article class="merge-card">
        <h4>构建写入的边（按来源组/行号回溯）</h4>
        <table class="data-table compact" data-testid="built-table">
          <thead>
            <tr>
              <th>边编号</th><th>来源组</th><th>来源行</th><th>关系</th>
              <th>主体（类型）</th><th>客体（类型）</th><th>构建任务</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="edge in detail.已建立边"
              :key="String(edge.边编号)"
              :data-testid="`built-${edge.来源组}-${edge.来源行}`"
            >
              <td>{{ edge.边编号 }}</td>
              <td>{{ edge.来源组 }}</td>
              <td>{{ edge.来源行 }}</td>
              <td>{{ edge.关系 }}</td>
              <td>{{ edge.主体键 }}（{{ edge.主体类型 }}）</td>
              <td>{{ edge.客体键 }}（{{ edge.客体类型 }}）</td>
              <td>{{ edge.构建任务编号 }}</td>
            </tr>
            <tr v-if="!detail.已建立边.length">
              <td colspan="7" class="empty-state">还没有派发出去的边</td>
            </tr>
          </tbody>
        </table>
      </article>

      <article class="merge-card">
        <h4>能继续领的行</h4>
        <table class="data-table compact" data-testid="claimable-table">
          <thead>
            <tr><th>来源组</th><th>来源行</th><th>关系</th><th>主体（类型）</th><th>客体（类型）</th></tr>
          </thead>
          <tbody>
            <tr
              v-for="row in detail.能继续领的行"
              :key="`${row.group_id}-${row.line_no}`"
              :data-testid="`claimable-${row.group_id}-${row.line_no}`"
            >
              <td>{{ row.group_id }}</td>
              <td>{{ row.line_no }}</td>
              <td>{{ row.relation }}</td>
              <td>{{ row.source }}（{{ row.source_type }}）</td>
              <td>{{ row.target }}（{{ row.target_type }}）</td>
            </tr>
            <tr v-if="!detail.能继续领的行.length">
              <td colspan="5" class="empty-state">暂无可以继续领的行</td>
            </tr>
          </tbody>
        </table>
      </article>

      <article class="merge-card">
        <h4>批量处理清单 · 派发记录</h4>
        <table class="data-table compact" data-testid="dispatch-table">
          <thead>
            <tr><th>派发编号</th><th>图谱构建任务</th><th>选中组</th><th>接收边数</th><th>待定边数</th><th>关联清单</th></tr>
          </thead>
          <tbody>
            <tr v-for="record in detail.dispatches" :key="String(record.dispatch_id)">
              <td>{{ record.dispatch_id }}</td>
              <td>{{ record.构建任务编号 }}</td>
              <td>{{ (record.选中组 as string[]).join('、') }}</td>
              <td>{{ record.接收边数 }}</td>
              <td>{{ record.待定边数 }}</td>
              <td>{{ record.关联清单 }}</td>
            </tr>
            <tr v-if="!detail.dispatches.length">
              <td colspan="6" class="empty-state">还没有派发记录</td>
            </tr>
          </tbody>
        </table>
      </article>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'

import {
  dispatchGroups,
  getBatch,
  listBatches,
  resolveCandidate,
} from '@/api/backend-service'
import type { BatchDetail, BatchSummary } from '@/api/backend-service'

const batches = ref<BatchSummary[]>([])
const emit = defineEmits<{ changed: [] }>()
const selectedBatchId = ref<number | null>(null)
const detail = ref<BatchDetail | null>(null)
const selectedGroups = ref<string[]>([])
const resolveKey = ref('')
const resolveType = ref('')
const busy = ref(false)
const mergeError = ref('')
const mergeNotice = ref('')

const allSelected = computed(() =>
  Boolean(detail.value) && selectedGroups.value.length === detail.value!.groups.length,
)

function isDetermined(key: string): boolean {
  return Boolean(detail.value?.determined_entities?.[key])
}

function syncGroupSelection() {
  selectedGroups.value = detail.value?.groups.map((group) => group.group_id) ?? []
}

async function loadBatches() {
  mergeError.value = ''
  try {
    batches.value = await listBatches()
    if (!selectedBatchId.value && batches.value.length) {
      selectedBatchId.value = batches.value[batches.value.length - 1].id
    }
    await loadBatch()
  } catch (error) {
    mergeError.value = error instanceof Error ? error.message : '批量处理清单读取失败'
  }
}

async function loadBatch() {
  if (!selectedBatchId.value) return
  mergeError.value = ''
  try {
    const previous = new Set(selectedGroups.value)
    detail.value = await getBatch(selectedBatchId.value)
    const ids = detail.value.groups.map((group) => group.group_id)
    selectedGroups.value = previous.size
      ? ids.filter((id) => previous.has(id))
      : ids
    if (!detail.value.待定端点.length) {
      const firstPending = detail.value.候选类型.find((c) => !isDetermined(c.key))
      resolveKey.value = firstPending?.key ?? ''
      resolveType.value = firstPending?.type ?? ''
    }
  } catch (error) {
    mergeError.value = error instanceof Error ? error.message : '批量处理清单读取失败'
  }
}

function toggleAll() {
  selectedGroups.value = allSelected.value
    ? []
    : detail.value?.groups.map((group) => group.group_id) ?? []
}

async function submitDispatch() {
  if (!selectedBatchId.value) return
  busy.value = true
  mergeError.value = ''
  mergeNotice.value = ''
  try {
    const result = await dispatchGroups(
      selectedBatchId.value,
      selectedGroups.value,
      '工作台局部派发',
    )
    mergeNotice.value = String(result.message ?? '派发完成')
    emit('changed')
    await loadBatches()
  } catch (error) {
    mergeError.value = error instanceof Error ? error.message : '局部派发失败'
  } finally {
    busy.value = false
  }
}

async function submitResolution() {
  if (!selectedBatchId.value || !resolveKey.value || !resolveType.value) return
  busy.value = true
  mergeError.value = ''
  mergeNotice.value = ''
  try {
    const result = await resolveCandidate(
      selectedBatchId.value,
      resolveKey.value,
      resolveType.value,
    )
    mergeNotice.value = String(result.message ?? '候选类型已纠正')
    emit('changed')
    await loadBatches()
  } catch (error) {
    mergeError.value = error instanceof Error ? error.message : '候选类型纠正失败'
  } finally {
    busy.value = false
  }
}

async function quickResolve(key: string, type: string) {
  resolveKey.value = key
  resolveType.value = type
  await submitResolution()
}

watch(selectedBatchId, () => {
  selectedGroups.value = []
})

onMounted(loadBatches)

defineExpose({ reload: loadBatches })
</script>

<style scoped>
.merge-panel { margin-top: 20px; display: flex; flex-direction: column; gap: 12px; }
.merge-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; }
.merge-head h3 { margin: 0; font-size: 16px; }
.merge-desc { margin: 4px 0 0; color: var(--muted); font-size: 12px; max-width: 760px; }
.merge-select { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.merge-select select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; }
.merge-notice { color: #1f6feb; font-size: 13px; margin: 0; }
.merge-columns { display: grid; grid-template-columns: 1.2fr 1fr; gap: 12px; }
.merge-card { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
.merge-card h4 { margin: 0 0 8px; font-size: 14px; }
.merge-card-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
.merge-actions { display: flex; gap: 8px; }
.merge-hint { color: var(--muted); font-size: 12px; margin: 0 0 8px; }
.pending-endpoints { display: flex; flex-direction: column; gap: 8px; }
.pending-endpoint { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; font-size: 13px; }
.pending-endpoint em { font-style: normal; margin-right: 8px; }
.muted { color: var(--muted); }
.tag { display: inline-block; border-radius: 10px; padding: 1px 8px; font-size: 12px; }
.tag.ok { background: #e8f5ec; color: #1a7f37; }
.tag.warn { background: #fff4e5; color: #b45309; }
.data-table.compact th, .data-table.compact td { padding: 6px 8px; font-size: 12px; }
@media (max-width: 1100px) { .merge-columns { grid-template-columns: 1fr; } }
</style>
