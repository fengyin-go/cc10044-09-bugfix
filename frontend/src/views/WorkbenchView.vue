<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <a
              v-if="meta.key === 'pipeline_monitor'"
              class="link"
              :data-testid="`result-${row.id}`"
              :href="`/pipeline_monitor/${row.id}`"
              @click.prevent="reload"
            >
              查看写入结果
            </a>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <section
      v-if="meta.key === 'pipeline_monitor' && selectedRun"
      class="detail-panel"
      data-testid="run-detail"
    >
      <h3>运行详情 · #{{ selectedRun.id }}</h3>
      <dl class="detail-grid">
        <div><dt>执行单号</dt><dd data-testid="detail-execution">{{ selectedRun['执行单号'] ?? '—' }}</dd></div>
        <div><dt>执行状态</dt><dd data-testid="detail-status">{{ selectedRun.status }}</dd></div>
        <div><dt>确认状态</dt><dd data-testid="detail-confirm">{{ selectedRun['确认状态'] ?? '—' }}</dd></div>
        <div><dt>处理行数</dt><dd data-testid="detail-rows">{{ selectedRun['处理行数'] ?? '0' }}</dd></div>
        <div><dt>写入数据范围</dt><dd data-testid="detail-written">{{ selectedRun['写入数据范围'] || '—' }}</dd></div>
        <div><dt>确认数据范围</dt><dd data-testid="detail-confirmed">{{ selectedRun['确认数据范围'] || '—' }}</dd></div>
      </dl>
      <button class="btn ghost" data-testid="detail-close" type="button" @click="selectedRun = null">
        关闭详情
      </button>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  listEntries,
  moduleMeta,
  runAction as applyAction,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string; runId?: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const selectedRun = ref<EntryRow | null>(null)
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  {
    label: meta.metrics[1],
    value: rows.value.filter((row) => successStatuses.value.includes(String(row.status))).length,
  },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => failureStatuses.value.includes(String(row.status))).length,
  },
])
const successStatuses = computed(
  () => meta.successStatuses ?? [meta.statuses[1], '已完成'],
)
const failureStatuses = computed(
  () => meta.failureStatuses ?? [meta.statuses[meta.statuses.length - 1], '执行失败', '超时终止'],
)
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
    selectedRun.value = rows.value.find((item) => Number(item.id) === Number(row.id)) ?? null
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

// 运行历史重新进入某条记录时，按 id 打开详情并与后端刷新后的值对齐。
function selectRunById(runId: string | null) {
  if (!runId || meta.key !== 'pipeline_monitor') return
  selectedRun.value = rows.value.find((row) => String(row.id) === runId) ?? null
}

onMounted(async () => {
  await reload()
  const routeRun = props.runId ?? window.location.pathname.match(/^\/[^/]+\/(\d+)$/)?.[1] ?? null
  selectRunById(routeRun ?? null)
})
</script>
