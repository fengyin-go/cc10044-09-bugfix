import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
    // 运行历史可直接深链到某条记录，刷新后仍能按 id 打开详情。
    { path: '/:moduleKey/:runId', name: 'module-run', component: WorkbenchView, props: true },
  ],
})

export default router
