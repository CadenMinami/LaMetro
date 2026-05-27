import { MetroMap } from '@/components/MetroMap';
import { AccountNav } from '@/components/AccountNav';

export default function Home() {
  return (
    <div className="relative h-screen w-screen overflow-hidden">
      <MetroMap />
      <AccountNav />
    </div>
  );
}
