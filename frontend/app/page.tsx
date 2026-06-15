import Link from 'next/link';
import { MetroMap } from '@/components/MetroMap';
import { AccountNav } from '@/components/AccountNav';

export default function Home() {
  return (
    <div className="relative h-screen w-screen overflow-hidden">
      <MetroMap />
      <AccountNav />
      <Link
        href="/equity"
        className="pointer-events-auto absolute bottom-4 left-4 z-[1000] rounded-full
                   bg-zinc-900/80 px-3 py-2 text-sm text-zinc-200 ring-1 ring-zinc-700
                   hover:bg-zinc-800"
      >
        Equity analysis →
      </Link>
    </div>
  );
}
