// Feishu/Lark Bot API Adapter using official SDK with WebSocket long polling
// Implements MessengerAdapter interface with// https://open.feishu.cn/document/server-side-sdk/nodejs-sdk/preparation-before-development

import type { MessengerAdapter, Message, MessageContext } from '../../../core/types.js'
import { FeishuClient } from './feishu-client.js'
import { CardBuilder } from './card-builder.js'
import type { FeishuConfig } from './types.js'
import { homedir } from 'os'
import { join } from 'path'
import { readFile } from 'fs/promises'

const CONFIG_FILE = join(homedir(), '.im-hub', 'config.json')
const PROCESSED_MESSAGES_TTL = 60 * 1000 // 1 minute

// Message event type from Feishu SDK
interface MessageReceiveEvent {
  message: {
    message_id: string
    root_id?: string
    parent_id?: string
    chat_id: string
    message_type: string
    content: string
    create_time: string
  }
  sender: {
    sender_id?: {
      open_id?: string
      user_id?: string
      union_id?: string
    }
    sender_type: string
    tenant_key: string
  }
}

export class FeishuAdapter implements MessengerAdapter {
  readonly name = 'feishu'
  private client: FeishuClient | null = null
  private config: FeishuConfig | null = null
  private messageHandler?: (ctx: MessageContext) => Promise<void>
  private isRunning = false
  private processedMessages = new Map<string, number>() // message_id -> timestamp

  async start(): Promise<void> {
    if (this.isRunning) {
      console.log('[Feishu] Adapter already running, skipping start')
      return
    }

    // Load config
    try {
      const data = await readFile(CONFIG_FILE, 'utf-8')
      const fullConfig = JSON.parse(data)
      this.config = fullConfig.feishu as FeishuConfig
    } catch {
      throw new Error('Feishu config not found. Run "im-hub config feishu" first.')
    }

    if (!this.config?.appId || !this.config?.appSecret) {
      throw new Error('Feishu App ID or Secret not configured. Run "im-hub config feishu" first.')
    }

    // Initialize client with WebSocket long polling
    this.client = new FeishuClient(this.config)

    // Set up message handler using official SDK event
    this.client.onMessage(async (event: MessageReceiveEvent) => {
      await this.handleFeishuMessage(event)
    })

    // Start WebSocket long polling
    await this.client.start()

    this.isRunning = true
    console.log('🚀 Feishu adapter started (WebSocket long polling mode)')
    console.log('   No webhook configuration needed!')
  }

  async stop(): Promise<void> {
    this.isRunning = false

    if (this.client) {
      await this.client.stop()
    }

    console.log('👋 Feishu adapter stopped')
  }

  onMessage(handler: (ctx: MessageContext) => Promise<void>): void {
    this.messageHandler = handler
  }

  async sendMessage(threadId: string, text: string): Promise<void> {
    if (!this.client) {
      throw new Error('Feishu adapter not started')
    }

    // Use card for better formatting
    const card = new CardBuilder()
      .addMarkdown(text)
      .build()

    await this.client.sendCard(threadId, card)
  }

  async sendCard(threadId: string, card: unknown): Promise<void> {
    if (!this.client) {
      throw new Error('Feishu adapter not started')
    }
    await this.client.sendCard(threadId, card)
  }

  async sendTyping(threadId: string, isTyping: boolean): Promise<void> {
    if (!this.client) {
      return
    }
    // Note: The typing indicator is handled by Feishu's built-in UI
    // when a message is being processed. No explicit API call needed.
    if (isTyping) {
      console.log('[Feishu] Processing message...')
    }
  }

  // ============================================
  // Event Handling
  // ============================================

  private async handleFeishuMessage(event: MessageReceiveEvent): Promise<void> {
    const message = event.message

    // Deduplicate messages - skip if already processed
    const msgId = message.message_id
    if (msgId && this.processedMessages.has(msgId)) {
      console.log('[Feishu] Skipping duplicate message:', msgId)
      return
    }

    console.log('[Feishu] Received message event')

    if (!this.messageHandler) {
      console.log('[Feishu] No message handler registered')
      return
    }

    const sender = event.sender

    // Skip bot messages
    if (sender.sender_type === 'app') {
      console.log('[Feishu] Ignoring bot message')
      return
    }

    // Parse message content
    let text = ''
    try {
      const content = JSON.parse(message.content || '{}')
      text = content.text || ''
    } catch {
      console.log('[Feishu] Failed to parse message content')
      return
    }

    if (!text) {
      console.log('[Feishu] Empty message text')
      return
    }

    console.log('[Feishu] Message:', text)

    // Mark message as processed
    if (msgId) {
      this.processedMessages.set(msgId, Date.now())
      this.cleanupProcessedMessages()
    }

    const msg: Message = {
      id: message.message_id || '',
      threadId: message.chat_id || '',
      userId: sender.sender_id?.open_id || sender.sender_id?.user_id || 'unknown',
      text,
      timestamp: new Date(parseInt(message.create_time || '0')),
      channelId: this.config?.channelId || 'default',
    }

    const ctx: MessageContext = {
      message: msg,
      platform: 'feishu',
      channelId: this.config?.channelId || 'default',
    }

    try {
      await this.messageHandler(ctx)
    } catch (error) {
      console.error('[Feishu] Error in message handler:', error)
    }
  }

  private cleanupProcessedMessages(): void {
    const cutoff = Date.now() - PROCESSED_MESSAGES_TTL
    for (const [id, timestamp] of this.processedMessages) {
      if (timestamp < cutoff) {
        this.processedMessages.delete(id)
      }
    }
  }
}

// Singleton instance
export const feishuAdapter = new FeishuAdapter()
